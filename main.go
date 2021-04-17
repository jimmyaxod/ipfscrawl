package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	noise "github.com/libp2p/go-libp2p-noise"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	secio "github.com/libp2p/go-libp2p-secio"
	libp2ptls "github.com/libp2p/go-libp2p-tls"

	"net/http"
	_ "net/http/pprof"
)

const (
	NUM_HOSTS = 4
)

// Example:

// https://cloudflare-ipfs.com/ipns/12D3KooWSPahV81xHimuUKrwNnonYgVrDMs1JtqmT3B12zsY5F6f
// https://cloudflare-ipfs.com/ipfs/QmTenMnimYgzfX96qdu1kHka1S68v9PxXi8pgHd29tJywT

func main() {

	go http.ListenAndServe("localhost:8080", nil)

	ctx := context.TODO()

	hosts := make([]host.Host, NUM_HOSTS)

	peerstore := pstoremem.NewPeerstore()

	for i := 0; i < NUM_HOSTS; i++ {
		hosts[i] = createHost(ctx, peerstore)
	}

	// Create a dht crawler using the above hosts
	dhtc := NewDHT(peerstore, hosts)

	/*
		targetID, err := peer.Decode("QmYHqPxxfrc4hFXUAqcWNtCu6E7BL7v5EitZdk4uUJekg2")
		if err != nil {
			fmt.Printf("error %v\n", err)
		}
		targetA, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/9000/quic")
		host.Peerstore().AddAddr(targetID, targetA, time.Hour)
		dhtc.Connect(targetID)
	*/

	addrs := dht.GetDefaultBootstrapPeerAddrInfos()
	// Put them in the peerstores...
	for _, addr := range addrs {
		for _, a := range addr.Addrs {
			fmt.Printf("Bootstrap %s %s\n", addr.ID, a)
		}
		for _, host := range hosts {
			host.Peerstore().AddAddrs(addr.ID, addr.Addrs, time.Hour)
		}
	}

	for _, addr := range addrs {
		dhtc.Connect(addr.ID)
	}

	ticker_stats := time.NewTicker(10 * time.Second)

	ticker_nuke_host := time.NewTicker(10 * time.Minute)

	for {
		select {
		case <-ticker_stats.C:
			dhtc.ShowStats()
		case <-ticker_nuke_host.C:
			// Create a new host, and replace it...
			myhost := createHost(ctx, peerstore)
			dhtc.ReplaceHost(myhost)
		}
	}

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
		myhost, err := libp2p.New(ctx,
			libp2p.Identity(priv),
			libp2p.ListenAddrStrings(
				fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),      // regular tcp connections
				fmt.Sprintf("/ip6/::/tcp/%d", port),           // regular tcp connections
				fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", port), // a UDP endpoint for the QUIC transport
				fmt.Sprintf("/ip6/::/udp/%d/quic", port),      // a UDP endpoint for the QUIC transport
			),
			libp2p.Security(libp2ptls.ID, libp2ptls.New),
			libp2p.Security(noise.ID, noise.New),
			libp2p.Security(secio.ID, secio.New),
			libp2p.Transport(libp2pquic.NewTransport),
			libp2p.Peerstore(peerstore),
			libp2p.DefaultTransports,
			libp2p.UserAgent("ipfscrawl"),
		)
		if err == nil {
			return myhost
		}
	}

}
