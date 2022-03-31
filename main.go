package main

/**
 * Simple DHT crawler
 *
 */

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	crawldht "github.com/jimmyaxod/ipfscrawl/dht"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"net/http"
	_ "net/http/pprof"
)

// Example:

// https://cloudflare-ipfs.com/ipns/12D3KooWSPahV81xHimuUKrwNnonYgVrDMs1JtqmT3B12zsY5F6f
// https://cloudflare-ipfs.com/ipfs/QmTenMnimYgzfX96qdu1kHka1S68v9PxXi8pgHd29tJywT

const PROMETHEUS_PORT = 2112

func main() {
	useDefaultBootstrap := flag.Bool("defaultbootstrap", false, "Use default bootstrap servers")
	bootstrap := flag.String("bootstrap", "", "Node to bootstrap from")
	NUM_HOSTS := flag.Int("hosts", 12, "Number of hosts to have running")
	flag.Parse()

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf(":%d", PROMETHEUS_PORT), nil)

	fmt.Printf("Listening on port %d...\n", PROMETHEUS_PORT)

	// So we can see pprof info
	//go http.ListenAndServe("0.0.0.0:8080", nil)

	ctx := context.TODO()

	hosts := make([]host.Host, *NUM_HOSTS)

	peerstore, _ := pstoremem.NewPeerstore()

	for i := 0; i < len(hosts); i++ {
		hosts[i] = createHost(ctx, peerstore)
	}

	// Create a dht crawler using the above hosts
	dhtc := crawldht.NewDHT(peerstore, hosts)

	if *bootstrap != "" {
		bits := strings.Split(*bootstrap, "@")
		id := bits[0]
		addr := bits[1]
		connect(dhtc, id, addr)
	}

	if *useDefaultBootstrap {
		addrs := dht.GetDefaultBootstrapPeerAddrInfos()
		// Put them in the peerstores...
		for _, addr := range addrs {
			for _, a := range addr.Addrs {
				fmt.Printf("Bootstrap %s %s\n", addr.ID, a)
			}
			peerstore.AddAddrs(addr.ID, addr.Addrs, 12*time.Hour)
			dhtc.Connect(addr.ID)
		}
	}

	fmt.Printf("Going into stats loop...\n")

	ticker_stats := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker_stats.C:
			dhtc.ShowStats()
			dhtc.UpdateStats()
		}
	}

}

// connect to something
func connect(dhtc *crawldht.DHT, id string, addr string) {
	fmt.Printf("Connecting to %s %s...\n", id, addr)
	targetID, err := peer.Decode(id)
	if err != nil {
		fmt.Printf("error %v\n", err)
	}
	targetA, err := multiaddr.NewMultiaddr(addr)
	dhtc.Peerstore.AddAddr(targetID, targetA, time.Hour)
	dhtc.Connect(targetID)
}

// Create a new host...
func createHost(ctx context.Context, peerstore peerstore.Peerstore) host.Host {
	// Create some keys
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		panic(err)
	}

	// Just try to create a host using a ton of ports until one works.
	// Lame, but who cares

	for {
		port := 4000 + rand.Intn(2000)

		// Create a new host...
		myhost, err := libp2p.New(
			libp2p.Identity(priv),
			libp2p.ListenAddrStrings(
				fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),      // regular tcp connections
				fmt.Sprintf("/ip6/::/tcp/%d", port),           // regular tcp connections
				fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", port), // a UDP endpoint for the QUIC transport
				fmt.Sprintf("/ip6/::/udp/%d/quic", port),      // a UDP endpoint for the QUIC transport
			),
			//			libp2p.Security(libp2ptls.ID, libp2ptls.New),
			//			libp2p.Security(noise.ID, noise.New),
			libp2p.Peerstore(peerstore),
			//libp2p.Transport(tcp.NewTCPTransport, tcp.WithConnectionTimeout(10*time.Second)),
			libp2p.DefaultTransports,
			libp2p.UserAgent("speeder0.01"),
			//			libp2p.ConnectionManager(connman),
		)

		ping.NewPingService(myhost)
		// ps is a ping service

		if err == nil {
			return myhost
		}
		fmt.Printf("Finding free port... %v \n", err)
	}
}
