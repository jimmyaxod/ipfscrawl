package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	mh "github.com/multiformats/go-multihash"
)

func main() {

	period := int64(60) // Every minute

	logPeers := NewOutputdata("peer_addrs", period)

	logPeerinfo := NewOutputdata("peer_protocols", period)

	logPeeragents := NewOutputdata("peer_agents", period)

	logPeerids := NewOutputdata("peer_ids", period)

	/*
		lvl, err := logging.LevelFromString("debug")
		if err != nil {
			panic(err)
		}
		logging.SetAllLoggers(lvl)
	*/

	num_crawlers := 128
	for cr := 0; cr < num_crawlers; cr++ {

		newcon := NewNetworkConn(cr)

		// Start one up...
		go func(con NetworkConn) {

			for {

				// Show some info...
				fmt.Printf("ID=%s peerstore=%d routingTable=%d\n", con.Host.ID(), len(con.Host.Peerstore().Peers()), len(con.DHT.RoutingTable().GetPeerInfos()))

				// Check out some data...

				// Do a lookup...
				ctx, _ := context.WithTimeout(context.TODO(), 4*time.Second)
				key := uuid.New().String()
				ch, err := con.DHT.GetClosestPeers(ctx, key)
				if err != nil {
					fmt.Printf("Hmm some error... %v\n", err)
				}

				go func() {
					for d := range ch {
						fmt.Printf("Walk result %v\n", d)

						// Look them up...
						ai, err := con.DHT.FindPeer(ctx, d)
						if err == nil {
							for _, a := range ai.Addrs {
								s := fmt.Sprintf("%s,%s", d, a)
								logPeers.WriteData(s)
							}
						}
						//

						protocols, err := con.Host.Peerstore().GetProtocols(ai.ID)
						if err == nil {
							for _, proto := range protocols {
								s := fmt.Sprintf("%s,%s", ai.ID, proto)
								logPeerinfo.WriteData(s)
							}
						} else {
							fmt.Printf("Can't get protocols %s %v\n", ai.ID, err)
						}

						agent, err := con.Host.Peerstore().Get(ai.ID, "AgentVersion")
						if err == nil {
							s := fmt.Sprintf("%s,%s", ai.ID, agent)
							logPeeragents.WriteData(s)
						}

						decoded, err := mh.Decode([]byte(ai.ID))
						if err == nil {
							s := fmt.Sprintf("%s,%s,%d,%x", ai.ID, decoded.Name, decoded.Length, decoded.Digest)
							logPeerids.WriteData(s)
						}
					}
				}()

				time.Sleep(100 * time.Millisecond)
			}
		}(newcon)
	}

	// Now sit and wait...

	time.Sleep(24 * time.Hour)
}
