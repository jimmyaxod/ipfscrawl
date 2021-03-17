package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
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
	hosts []host.Host

	started time.Time

	metric_con_outgoing_fail    uint64
	metric_con_outgoing_success uint64
	metric_con_incoming         uint64

	metric_written_ping      uint64
	metric_written_find_node uint64

	metric_read_put_value    uint64
	metric_read_get_value    uint64
	metric_read_add_provider uint64
	metric_read_get_provider uint64
	metric_read_find_node    uint64
	metric_read_ping         uint64

	metric_peers_found uint64
}

// NewDHT creates a new DHT ontop of the given host
func NewDHT(hosts []host.Host) DHT {
	dht := DHT{
		hosts:   hosts,
		started: time.Now(),
	}

	// Set it up to handle incoming streams...
	for _, host := range hosts {
		host.SetStreamHandler(proto, dht.handleNewStream)
	}

	return dht
}

func (dht *DHT) ShowStats() {
	fmt.Printf("DHT uptime=%.2fs total_peers_found=%d\n", time.Since(dht.started).Seconds(), dht.metric_peers_found)
	fmt.Printf("Connections out=%d (%d fails) in=%d\n", dht.metric_con_outgoing_success, dht.metric_con_outgoing_fail, dht.metric_con_incoming)
	fmt.Printf("Writes ping=%d find_node=%d\n", dht.metric_written_ping, dht.metric_written_find_node)

	// Stats on incoming messages
	fmt.Printf("Reads put=%d get=%d addprov=%d getprov=%d find_node=%d ping=%d\n",
		dht.metric_read_put_value, dht.metric_read_get_value,
		dht.metric_read_add_provider, dht.metric_read_get_provider,
		dht.metric_read_find_node, dht.metric_read_ping)

	for i, host := range dht.hosts {
		fmt.Printf("Host[%d %s] Peerstore=%d\n", i, host.ID(), host.Peerstore().Peers().Len())
	}
	fmt.Println()
}

// Connect connects to a new peer, and does some stuff
func (dht *DHT) Connect(id peer.ID) error {
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
	//	fmt.Printf("handleNewStream %v\n", s)
	atomic.AddUint64(&dht.metric_con_incoming, 1)

	dht.ProcessPeerStream(s)
}

// Process a peer stream
func (dht *DHT) ProcessPeerStream(s network.Stream) {
	//	fmt.Printf("ProcessPeerStream %v\n", s)

	// Lets only use the connection for so long... 10 minutes?
	ctx, cancelFunc := context.WithTimeout(context.TODO(), 10*time.Minute)

	// Info on the connection
	//	rPeer := s.Conn().RemotePeer()

	// Setup something to periodically write PING and FIND_NODE
	go func() {
		defer cancelFunc()

		// Setup reader / writer
		w := msgio.NewVarintWriter(s)

		ticker_ping := time.NewTicker(10 * time.Second)
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
				// Send out a find_node

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
	}()

	// Reader...
	go func() {
		defer cancelFunc()

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

			// Incoming message!
			//			fmt.Printf("Msg peer=%s type=%s key=%s\n", rPeer, req.GetType(), req.GetKey())

			switch req.GetType() {
			case 0:
				atomic.AddUint64(&dht.metric_read_put_value, 1)
			case 1:
				atomic.AddUint64(&dht.metric_read_get_value, 1)
			case 2:
				atomic.AddUint64(&dht.metric_read_add_provider, 1)
			case 3:
				atomic.AddUint64(&dht.metric_read_get_provider, 1)
			case 4:
				atomic.AddUint64(&dht.metric_read_find_node, 1)
			case 5:
				atomic.AddUint64(&dht.metric_read_ping, 1)
			}

			// Check out the FIND_NODE on that!
			for _, cpeer := range req.CloserPeers {

				// Carry on the crawl...

				pid, err := peer.IDFromBytes([]byte(cpeer.Id))
				if err == nil {

					// For now, we'll add it to all host peerstores...
					for _, host := range dht.hosts {

						for _, a := range cpeer.Addrs {
							ad, err := multiaddr.NewMultiaddrBytes(a)
							if err == nil {
								host.Peerstore().AddAddr(pid, ad, 1*time.Hour)
							}
						}

					}
					atomic.AddUint64(&dht.metric_peers_found, 1)

					// Now lets go to this one...
					// Aint no way I'm waiting though
					go dht.Connect(pid)
				}
			}
		}
	}()
}
