package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"

	peerstore "github.com/libp2p/go-libp2p-peerstore"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	mh "github.com/multiformats/go-multihash"

	ipnspb "github.com/ipfs/go-ipns/pb"
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

type DHT struct {
	peerstore peerstore.Peerstore
	hosts     []host.Host

	started time.Time

	metric_con_outgoing_fail    uint64
	metric_con_outgoing_success uint64
	metric_con_outgoing_dupe    uint64
	metric_con_incoming         uint64
	metric_written_ping         uint64
	metric_written_find_node    uint64
	metric_read_put_value       uint64
	metric_read_get_value       uint64
	metric_read_add_provider    uint64
	metric_read_get_provider    uint64
	metric_read_find_node       uint64
	metric_read_ping            uint64
	metric_peers_found          uint64

	log_peerinfo       Outputdata
	log_addproviders   Outputdata
	log_getproviders   Outputdata
	log_put            Outputdata
	log_get            Outputdata
	log_put_ipns       Outputdata
	log_peer_protocols Outputdata
	log_peer_agents    Outputdata
	log_peer_ids       Outputdata

	mu             sync.Mutex
	activePeers    map[string]bool
	numActivePeers int64
}

// NewDHT creates a new DHT on top of the given hosts
func NewDHT(peerstore peerstore.Peerstore, hosts []host.Host) *DHT {
	output_file_period := int64(60 * 60)

	dht := &DHT{
		peerstore:          peerstore,
		hosts:              hosts,
		started:            time.Now(),
		log_peerinfo:       NewOutputdata("peerinfo", output_file_period),
		log_peer_protocols: NewOutputdata("peerprotocols", output_file_period),
		log_peer_agents:    NewOutputdata("peeragents", output_file_period),
		log_peer_ids:       NewOutputdata("peerids", output_file_period),
		log_addproviders:   NewOutputdata("addproviders", output_file_period),
		log_getproviders:   NewOutputdata("getproviders", output_file_period),
		log_put:            NewOutputdata("put", output_file_period),
		log_get:            NewOutputdata("get", output_file_period),
		log_put_ipns:       NewOutputdata("put_ipns", output_file_period),
		activePeers:        make(map[string]bool),
	}

	// Set it up to handle incoming streams of the correct protocol
	for _, host := range hosts {
		host.SetStreamHandler(proto, dht.handleNewStream)
	}

	return dht
}

// ShowStats - print out some stats about our crawl
func (dht *DHT) ShowStats() {
	// How many connections do we have?, how many streams?
	total_connections := 0
	total_streams := 0
	total_peerstore := dht.peerstore.Peers().Len()

	for _, host := range dht.hosts {
		cons := host.Network().Conns()

		total_connections += len(cons)
		for _, con := range cons {
			total_streams += len(con.GetStreams())
		}
	}

	active := atomic.LoadInt64(&dht.numActivePeers)

	fmt.Printf("DHT uptime=%.2fs active=%d total_peers_found=%d\n", time.Since(dht.started).Seconds(), active, dht.metric_peers_found)
	fmt.Printf("Current Hosts cons=%d streams=%d peerstore=%d\n", total_connections, total_streams, total_peerstore)
	fmt.Printf("Total Connections out=%d (%d fails) (%d dupes) in=%d\n", dht.metric_con_outgoing_success, dht.metric_con_outgoing_fail, dht.metric_con_outgoing_dupe, dht.metric_con_incoming)
	fmt.Printf("Total Writes ping=%d find_node=%d\n", dht.metric_written_ping, dht.metric_written_find_node)

	// Stats on incoming messages
	fmt.Printf("Reads put=%d get=%d addprov=%d getprov=%d find_node=%d ping=%d\n",
		dht.metric_read_put_value, dht.metric_read_get_value,
		dht.metric_read_add_provider, dht.metric_read_get_provider,
		dht.metric_read_find_node, dht.metric_read_ping)

	fmt.Println()
}

// Connect connects to a new peer, and starts an eventloop for it
func (dht *DHT) Connect(id peer.ID) error {
	// If we are already connected to the peerID, don't bother
	dht.mu.Lock()
	v, ok := dht.activePeers[id.Pretty()]
	dht.mu.Unlock()

	if v && ok {
		atomic.AddUint64(&dht.metric_con_outgoing_dupe, 1)
		return nil
	}

	// Pick a host at random...
	host := dht.hosts[rand.Intn(len(dht.hosts))]

	s, err := host.NewStream(context.TODO(), id, proto)
	if err != nil {
		atomic.AddUint64(&dht.metric_con_outgoing_fail, 1)
		return err
	}
	atomic.AddUint64(&dht.metric_con_outgoing_success, 1)
	dht.ProcessPeerStream(s)
	return nil
}

// handleNewStream handles incoming streams
func (dht *DHT) handleNewStream(s network.Stream) {
	atomic.AddUint64(&dht.metric_con_incoming, 1)
	dht.ProcessPeerStream(s)
}

// doPeriodicWrites handles sending periodic FIND_NODE and PING
func (dht *DHT) doPeriodicWrites(ctx context.Context, cancelFunc context.CancelFunc, s io.Writer) {
	defer cancelFunc()

	w := msgio.NewVarintWriter(s)

	ticker_ping := time.NewTicker(30 * time.Second)
	ticker_find_node := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker_ping.C:
			// Send out a ping
			msg_ping := pb.Message{
				Type: pb.Message_PING,
			}

			data_ping, err := msg_ping.Marshal()
			if err != nil {
				return
			}
			err = w.WriteMsg(data_ping)
			if err != nil {
				return
			}
			atomic.AddUint64(&dht.metric_written_ping, 1)
		case <-ticker_find_node.C:
			// Send out a find_node for a random key

			key := make([]byte, 16)
			rand.Read(key)

			msg := pb.Message{
				Type: pb.Message_FIND_NODE,
				Key:  key,
			}

			data, err := msg.Marshal()
			if err != nil {
				return
			}
			err = w.WriteMsg(data)
			if err != nil {
				return
			}
			atomic.AddUint64(&dht.metric_written_find_node, 1)
		case <-ctx.Done():
			return
		}
	}
}

// doReading reads msgs from stream and processes...
func (dht *DHT) doReading(ctx context.Context, cancelFunc context.CancelFunc, s network.Stream, peerID string) {
	defer cancelFunc()

	// Close the stream in the reader only...
	defer s.Close()

	// When we're done reading, we'll remove from activePeers...
	defer func() {
		dht.mu.Lock()
		delete(dht.activePeers, peerID)
		atomic.AddInt64(&dht.numActivePeers, -1)
		dht.mu.Unlock()
	}()

	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	for {
		var req pb.Message
		msgbytes, err := r.ReadMsg()
		if err != nil {
			return
		}

		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			return
		}

		switch req.GetType() {
		case 0:
			atomic.AddUint64(&dht.metric_read_put_value, 1)

			rec := req.GetRecord()

			s := fmt.Sprintf("%s,%x,%x,%x,%s", peerID, req.GetKey(), rec.Key, rec.Value, rec.TimeReceived)
			dht.log_put.WriteData(s)

			// TODO: /pk/ - maps Hash(pubkey) to pubkey

			// If it's ipns...
			if strings.HasPrefix(string(rec.Key), "/ipns/") {
				// Extract the PID...
				pidbytes := rec.Key[6:]
				pid, err := peer.IDFromBytes(pidbytes)
				if err == nil {
					// ok so it's an ipns record, so lets examine the data...
					ipns_rec := ipnspb.IpnsEntry{}
					err = ipns_rec.Unmarshal(rec.Value)
					if err == nil {
						// Log everything...
						//pubkey := ipns_rec.GetPubKey()
						//validity := ipns_rec.GetValidity()
						//signature := ipns_rec.GetSignature()

						ttl := ipns_rec.GetTtl()
						seq := ipns_rec.GetSequence()
						value := ipns_rec.GetValue()

						s := fmt.Sprintf("%s,%s,%d,%d", pid.Pretty(), string(value), ttl, seq)
						dht.log_put_ipns.WriteData(s)

					} else {
						fmt.Printf("Error unmarshalling ipns %v\n", err)
					}
				}
			}

		case 1:
			atomic.AddUint64(&dht.metric_read_get_value, 1)

			s := fmt.Sprintf("%s,%x", peerID, req.GetKey())
			dht.log_get.WriteData(s)

		case 2:
			atomic.AddUint64(&dht.metric_read_add_provider, 1)

			pinfos := pb.PBPeersToPeerInfos(req.GetProviderPeers())

			_, cid, err := cid.CidFromBytes(req.GetKey())
			if err == nil {

				// Log all providers...
				for _, pi := range pinfos {
					for _, a := range pi.Addrs {
						s := fmt.Sprintf("%s,%s,%s,%s", peerID, cid, pi.ID, a)
						dht.log_addproviders.WriteData(s)
					}
				}
			}
		case 3:
			atomic.AddUint64(&dht.metric_read_get_provider, 1)

			_, cid, err := cid.CidFromBytes(req.GetKey())
			if err == nil {
				s := fmt.Sprintf("%s,%s", peerID, cid)
				dht.log_getproviders.WriteData(s)
			}
		case 4:
			atomic.AddUint64(&dht.metric_read_find_node, 1)
		case 5:
			atomic.AddUint64(&dht.metric_read_ping, 1)
			// We need to send back a PONG...
		}

		// Check out the FIND_NODE on that!
		for _, cpeer := range req.CloserPeers {

			pid, err := peer.IDFromBytes([]byte(cpeer.Id))
			if err == nil {
				for _, a := range cpeer.Addrs {
					ad, err := multiaddr.NewMultiaddrBytes(a)
					if err == nil && isConnectable(ad) {

						dht.peerstore.AddAddr(pid, ad, 12*time.Hour)

						// fromPeerID, newPeerID, addr
						s := fmt.Sprintf("%s,%s,%s", peerID, pid, ad)
						dht.log_peerinfo.WriteData(s)
					}
				}

				atomic.AddUint64(&dht.metric_peers_found, 1)

				// Now lets go to this one...
				// Aint no way I'm waiting though
				go dht.Connect(pid)
			}
		}
	}
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

// Process a peer stream
func (dht *DHT) ProcessPeerStream(s network.Stream) {
	pid := s.Conn().RemotePeer()
	peerID := pid.Pretty()

	// Add it in to activePeers, so we don't have dupe connections
	dht.mu.Lock()
	v, ok := dht.activePeers[peerID]
	if v && ok {
		dht.mu.Unlock()
		s.Close()
		return
	}
	dht.activePeers[peerID] = true
	atomic.AddInt64(&dht.numActivePeers, 1)
	dht.mu.Unlock()

	host := dht.hosts[0] // For now, since they all use the same anyways...

	// Find out some info about the peer...
	protocols, err := host.Peerstore().GetProtocols(pid)
	if err == nil {
		for _, proto := range protocols {
			s := fmt.Sprintf("%s,%s", peerID, proto)
			dht.log_peer_protocols.WriteData(s)
		}
	}

	agent, err := host.Peerstore().Get(pid, "AgentVersion")
	if err == nil {
		s := fmt.Sprintf("%s,%s", peerID, agent)
		dht.log_peer_agents.WriteData(s)
	}

	decoded, err := mh.Decode([]byte(pid))
	if err == nil {
		s := fmt.Sprintf("%s,%s,%d,%x", peerID, decoded.Name, decoded.Length, decoded.Digest)
		dht.log_peer_ids.WriteData(s)
	}

	// Lets only use the connection for so long... 10 minutes?
	ctx, cancelFunc := context.WithTimeout(context.TODO(), 10*time.Minute)

	// Start something up to periodically FIND_NODE and PING
	go dht.doPeriodicWrites(ctx, cancelFunc, s)

	// Start something to read messages
	go dht.doReading(ctx, cancelFunc, s, peerID)
}
