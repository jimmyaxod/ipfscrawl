package dhtc

import (
	"context"
	"fmt"

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
 */

const (
	proto = "/ipfs/kad/1.0.0"
)

type DHT struct {
	host host.Host
}

func NewDHT(host host.Host) DHT {
	dht := DHT{
		host: host,
	}

	// Set it up to handle incoming streams...
	host.SetStreamHandler(proto, dht.handleNewStream)

	return dht
}

// Connect connects to a new peer, and does some stuff
func (dht *DHT) Connect(id peer.ID) {
	s, err := dht.host.NewStream(context.TODO(), id, proto)
	if err != nil {
		fmt.Printf("Error Connecting %v\n", err)
	} else {
		// Handle it same as incoming
		dht.handleNewStream(s)
	}
}

func (dht *DHT) handleNewStream(s network.Stream) {
	fmt.Printf("handleNewStream %v\n", s)

	go func() {
		r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
		w := msgio.NewVarintWriter(s)

		//		rw := msgio.NewReadWriter(s)

		mPeer := s.Conn().RemotePeer()

		// Lets try a query!

		// Just use our own ID for now...
		key := []byte(dht.host.ID())

		msg := pb.Message{
			Type: pb.Message_FIND_NODE,
			Key:  key,
		}

		msg_ping := pb.Message{
			Type: pb.Message_PING,
		}

		data_ping, err := msg_ping.Marshal()
		if err != nil {
			fmt.Printf("Error marshaling msg %v\n", err)
		}
		fmt.Printf("WRITE %x\n", data_ping)
		err = w.WriteMsg(data_ping)
		if err != nil {
			fmt.Printf("Error on write %v\n", err)
		}

		data, err := msg.Marshal()
		if err != nil {
			fmt.Printf("Error marshaling msg %v\n", err)
		}
		fmt.Printf("WRITE %x\n", data)
		err = w.WriteMsg(data)
		if err != nil {
			fmt.Printf("Error on write %v\n", err)
		}

		for {
			var req pb.Message
			msgbytes, err := r.ReadMsg()

			if err != nil {
				fmt.Printf("Error in stream %v\n", err)
				return
			}

			fmt.Printf("READ %x\n", msgbytes)

			err = req.Unmarshal(msgbytes)
			r.ReleaseMsg(msgbytes)
			if err != nil {
				fmt.Println("Error unmarshal")
				return
			}

			fmt.Printf("Msg peer=%s type=%s key=%s\n", mPeer, req.GetType(), req.GetKey())

			// Check out the FIND_NODE on that!
			for _, cpeer := range req.CloserPeers {

				// Carry on the crawl...

				pid, err := peer.IDFromBytes([]byte(cpeer.Id))
				if err != nil {
					fmt.Printf("Error pid %v\n", err)
				}
				fmt.Printf(" Peer found %s\n", pid)
				for _, a := range cpeer.Addrs {
					ad, err := multiaddr.NewMultiaddrBytes(a)
					if err != nil {
						fmt.Printf("Error in addr %v\n", err)
					}
					fmt.Printf(" - %s\n", ad)
				}
			}
		}
	}()
}
