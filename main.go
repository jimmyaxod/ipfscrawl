package main

/**
 * Simple ipfs DHT crawler
 *
 */

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jimmyaxod/ipfscrawl/bitswap"
	crawldht "github.com/jimmyaxod/ipfscrawl/dht"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-tcp-transport"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"net/http"
	_ "net/http/pprof"
)

// Example:

// https://cloudflare-ipfs.com/ipns/12D3KooWSPahV81xHimuUKrwNnonYgVrDMs1JtqmT3B12zsY5F6f
// https://cloudflare-ipfs.com/ipfs/QmTenMnimYgzfX96qdu1kHka1S68v9PxXi8pgHd29tJywT

const PERIOD_FIND_NODE = 200 * time.Millisecond
const MAX_NODE_DETAILS = 10000

const PROMETHEUS_PORT = 2112

func main() {
	useDefaultBootstrap := flag.Bool("defaultbootstrap", false, "Use default bootstrap servers")
	bootstrap := flag.String("bootstrap", "", "Node to bootstrap from")
	NUM_HOSTS := flag.Int("hosts", 12, "Number of hosts to have running")
	flag.Parse()

	fmt.Printf("Setting up prometheus /metrics\n")
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf(":%d", PROMETHEUS_PORT), nil)
	fmt.Printf("Listening on port %d...\n", PROMETHEUS_PORT)

	// So we can see pprof info
	//go http.ListenAndServe("0.0.0.0:8080", nil)

	ctx := context.TODO()

	fmt.Printf("Setting up %d hosts\n", *NUM_HOSTS)
	hosts := make([]host.Host, *NUM_HOSTS)

	peerstore, _ := pstoremem.NewPeerstore()

	for i := 0; i < len(hosts); i++ {
		hosts[i] = createHost(ctx, peerstore)
	}

	nodeDetails := *crawldht.NewNodeDetails(MAX_NODE_DETAILS, peerstore)
	sessionMgrBitswap := bitswap.NewBitswapSessionMgr(hosts)
	sessionMgrDHT := crawldht.NewDHTSessionMgr(hosts, sessionMgrBitswap, &nodeDetails)

	if *bootstrap != "" {
		bits := strings.Split(*bootstrap, "@")
		id := bits[0]
		addr := bits[1]
		host := hosts[rand.Intn(len(hosts))]
		connect(host, peerstore, sessionMgrDHT, id, addr)
	}

	if *useDefaultBootstrap {
		addrs := dht.GetDefaultBootstrapPeerAddrInfos()
		// Put them in the peerstores...
		for _, addr := range addrs {
			peerstore.AddAddrs(addr.ID, addr.Addrs, 12*time.Hour)
			fmt.Printf("Bootstrap to %s\n", addr.ID.Pretty())
			host := hosts[rand.Intn(len(hosts))]
			sessionMgrDHT.SendRandomFindNode(host, addr.ID)
		}
	}

	fmt.Printf("Going into wait loop...\n")

	// Start something to keep us alive with outgoing find_node calls
	find_node_ticker := time.NewTicker(PERIOD_FIND_NODE)

	for {
		select {
		case <-find_node_ticker.C:
			// Find something to connect to
			targetID := nodeDetails.Get()
			host := hosts[rand.Intn(len(hosts))]
			go sessionMgrDHT.SendRandomFindNode(host, targetID)
		}
	}

}

// connect to something
func connect(host host.Host, pstore peerstore.Peerstore, mgr *crawldht.DHTSessionMgr, id string, addr string) {
	fmt.Printf("Connecting to %s %s...\n", id, addr)
	targetID, err := peer.Decode(id)
	if err != nil {
		fmt.Printf("error %v\n", err)
	}
	targetA, err := multiaddr.NewMultiaddr(addr)
	pstore.AddAddr(targetID, targetA, time.Hour)
	mgr.SendRandomFindNode(host, targetID)
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
			libp2p.Peerstore(peerstore),
			libp2p.Transport(tcp.NewTCPTransport, tcp.WithConnectionTimeout(10*time.Second)),
			libp2p.UserAgent("speeder0.01"),
			// libp2p.Security(libp2ptls.ID, libp2ptls.New),
			// libp2p.Security(noise.ID, noise.New),
			// libp2p.DefaultTransports,
			// libp2p.ConnectionManager(connman),
		)

		ping.NewPingService(myhost)
		// ps is a ping service

		if err == nil {
			return myhost
		}
		fmt.Printf("Finding free port... %v \n", err)
	}
}
