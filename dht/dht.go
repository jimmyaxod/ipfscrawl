package dht

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"

	"github.com/multiformats/go-multiaddr"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

/**
 * Here we're going to make a bare bones kad dht impl to play with the DHT
 *
 * We traverse the network by sending out FIND_NODE for random IDs. We get responses
 * with closer peers. We then continue with them.
 *
 * As we crawl the network, we also start getting other messages (eg GET_PROVIDERS)
 */

const (
	proto = "/ipfs/kad/1.0.0"
)

const CONNECTION_MAX_TIME = 10 * time.Second
const PERIOD_FIND_NODE = 10 * time.Millisecond

const MAX_NODE_DETAILS = 10000

var (
	p_con_outgoing_fail = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_con_outgoing_fail", Help: ""})
	p_con_outgoing_success = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_con_outgoing_success", Help: ""})

	p_con_incoming = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_con_incoming", Help: ""})
	p_con_incoming_rejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_con_incoming_rejected", Help: ""})
)

type DHT struct {
	sessionMgr  *DHTSessionMgr
	nodedetails NodeDetails

	target_peerstore int

	Peerstore peerstore.Peerstore
	hosts     []host.Host

	started time.Time
}

// NewDHT creates a new DHT on top of the given hosts
func NewDHT(peerstore peerstore.Peerstore, hosts []host.Host) *DHT {
	dht := &DHT{
		nodedetails: *NewNodeDetails(MAX_NODE_DETAILS, peerstore),
		hosts:       hosts,
		Peerstore:   peerstore,
		started:     time.Now(),
	}

	dht.sessionMgr = NewDHTSessionMgr(&dht.nodedetails)

	// Set it up to handle incoming streams of the correct protocol
	for _, host := range hosts {
		host.SetStreamHandler(proto, dht.handleNewStream)
	}

	// Start something to keep us alive with outgoing find_node calls
	go func() {
		find_node_ticker := time.NewTicker(PERIOD_FIND_NODE)

		for {
			select {
			case <-find_node_ticker.C:
				// Find something to connect to
				targetID := dht.nodedetails.Get()
				go dht.Connect(targetID)
			}
		}
	}()

	return dht
}

// Connect connects to a new peer, and starts an eventloop for it
// Assumes that tryConnectTo has already been called...
func (dht *DHT) Connect(id peer.ID) error {
	//	fmt.Printf("Outgoing [%s]\n", id.Pretty())

	dht.nodedetails.Add(id.Pretty())

	// Pick a host at random...
	host := dht.hosts[rand.Intn(len(dht.hosts))]

	ctx, _ := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	s, err := host.NewStream(ctx, id, proto)
	if err != nil {
		p_con_outgoing_fail.Inc()
		dht.nodedetails.ConnectFailure(id.Pretty())
		return err
	}

	p_con_outgoing_success.Inc()
	dht.nodedetails.Connected(id.Pretty())
	dht.nodedetails.ConnectSuccess(id.Pretty())

	pid := s.Conn().RemotePeer()

	dht.nodedetails.WritePeerInfo(pid)

	ses := NewDHTSession(ctx, dht.sessionMgr, s)
	go func() {
		msg := ses.MakeRandomFindNode()
		resp, err := ses.SendMsg(msg)
		if err == nil {
			if resp.GetType() == pb.Message_FIND_NODE {
				peerID := ses.stream.Conn().RemotePeer().Pretty()
				localPeerID := ses.stream.Conn().LocalPeer().Pretty()

				if len(resp.CloserPeers) > 0 {
					ses.Log(fmt.Sprintf("reader CloserPeers %d", len(resp.CloserPeers)))
					// Check out the FIND_NODE on that!
					for _, cpeer := range resp.CloserPeers {

						pid, err := peer.IDFromBytes([]byte(cpeer.Id))
						if err == nil {
							for _, a := range cpeer.Addrs {
								ad, err := multiaddr.NewMultiaddrBytes(a)
								if err == nil && isConnectable(ad) {
									ses.mgr.NotifyCloserPeers(localPeerID, peerID, pid, ad)
								}
							}
						}
					}
				}
			} else {
				ses.Log(fmt.Sprintf("Unexpected message"))
			}
		}
		dht.nodedetails.Disconnected(pid.Pretty())
	}()

	return nil
}

// handleNewStream handles incoming streams
func (dht *DHT) handleNewStream(s network.Stream) {
	pid := s.Conn().RemotePeer()

	//	fmt.Printf("Incoming [%s]\n", pid.Pretty())
	dht.nodedetails.Add(pid.Pretty())
	dht.nodedetails.ConnectSuccess(pid.Pretty())
	dht.nodedetails.Connected(pid.Pretty())

	p_con_incoming.Inc()

	// TODO, maybe...
	// p_con_incoming_rejected.Inc()

	ctx, _ := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	dht.nodedetails.WritePeerInfo(pid)

	ses := NewDHTSession(ctx, dht.sessionMgr, s)
	go func() {
		ses.Handle()
		dht.nodedetails.Disconnected(pid.Pretty())
	}()
}

// Filter out some common unconnectable addresses...
func isConnectable(a multiaddr.Multiaddr) bool {

	// Loopbacks
	if strings.HasPrefix(a.String(), "/ip4/127.0.0.1/") ||
		strings.HasPrefix(a.String(), "/ip6/::1/") {
		return false
	}

	// Internal ip4 ranges
	if strings.HasPrefix(a.String(), "/ip4/192.168.") ||
		strings.HasPrefix(a.String(), "/ip4/10.") ||
		strings.HasPrefix(a.String(), "/ip4/172.16.") ||
		strings.HasPrefix(a.String(), "/ip4/172.17.") ||
		strings.HasPrefix(a.String(), "/ip4/172.18.") ||
		strings.HasPrefix(a.String(), "/ip4/172.19.") ||
		strings.HasPrefix(a.String(), "/ip4/172.20.") ||
		strings.HasPrefix(a.String(), "/ip4/172.21.") ||
		strings.HasPrefix(a.String(), "/ip4/172.22.") ||
		strings.HasPrefix(a.String(), "/ip4/172.23.") ||
		strings.HasPrefix(a.String(), "/ip4/172.24.") ||
		strings.HasPrefix(a.String(), "/ip4/172.25.") ||
		strings.HasPrefix(a.String(), "/ip4/172.26.") ||
		strings.HasPrefix(a.String(), "/ip4/172.27.") ||
		strings.HasPrefix(a.String(), "/ip4/172.28.") ||
		strings.HasPrefix(a.String(), "/ip4/172.29.") ||
		strings.HasPrefix(a.String(), "/ip4/172.30.") ||
		strings.HasPrefix(a.String(), "/ip4/172.31.") {
		return false
	}
	return true
}
