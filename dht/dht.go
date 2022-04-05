package dht

import (
	"context"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

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
	protocol_dht = "/ipfs/kad/1.0.0"
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

	dht.sessionMgr = NewDHTSessionMgr(dht, &dht.nodedetails)

	// Set it up to handle incoming streams of the correct protocol
	for _, host := range hosts {
		host.SetStreamHandler(protocol_dht, dht.handleNewStream)
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

func (dht *DHT) Connect(id peer.ID) error {
	dht.nodedetails.Add(id.Pretty())

	// Pick a host at random...
	host := dht.hosts[rand.Intn(len(dht.hosts))]
	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	s, err := host.NewStream(ctx, id, protocol_dht)
	if err != nil {
		p_con_outgoing_fail.Inc()
		dht.nodedetails.ConnectFailure(id.Pretty())
		cancelFunc()
		return err
	}

	p_con_outgoing_success.Inc()
	dht.nodedetails.Connected(id.Pretty())
	dht.nodedetails.ConnectSuccess(id.Pretty())

	pid := s.Conn().RemotePeer()

	dht.nodedetails.WritePeerInfo(pid)

	ses := NewDHTSession(ctx, cancelFunc, dht.sessionMgr, s)
	go func() {
		msg := ses.MakeRandomFindNode()
		ses.SendMsg(msg)
		// Don't need to do anything about the reply
		dht.nodedetails.Disconnected(pid.Pretty())
		cancelFunc()
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

	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	dht.nodedetails.WritePeerInfo(pid)

	ses := NewDHTSession(ctx, cancelFunc, dht.sessionMgr, s)
	go func() {
		ses.Handle()
		dht.nodedetails.Disconnected(pid.Pretty())
		cancelFunc()
	}()
}
