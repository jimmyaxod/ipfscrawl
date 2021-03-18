package main

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	secio "github.com/libp2p/go-libp2p-secio"
	libp2ptls "github.com/libp2p/go-libp2p-tls"
)

const (
	NUM_HOSTS = 32
)

func main() {

	ctx := context.TODO()

	hosts := make([]host.Host, NUM_HOSTS)

	for i := 0; i < NUM_HOSTS; i++ {

		priv, _, err := crypto.GenerateKeyPair(
			crypto.RSA, // Select your key type. Ed25519 are nice short
			2048,       // Select key length when possible (i.e. RSA).
		)
		if err != nil {
			panic(err)
		}

		// Create a new host...
		myhost, err := libp2p.New(ctx,
			libp2p.Identity(priv),
			libp2p.ListenAddrStrings(
				fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", 7000+i),      // regular tcp connections
				fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", 7000+i), // a UDP endpoint for the QUIC transport
			),
			libp2p.Security(libp2ptls.ID, libp2ptls.New),
			libp2p.Security(secio.ID, secio.New),
			libp2p.Transport(libp2pquic.NewTransport),
			libp2p.DefaultTransports,
		)
		if err != nil {
			panic("Can't create host")
		}
		hosts[i] = myhost
	}

	// Create a dht crawler using the above hosts
	dhtc := NewDHT(hosts)

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

	for {
		select {
		case <-ticker_stats.C:
			dhtc.ShowStats()
		}
	}

	/*
		targetID, err := peer.Decode("QmYHqPxxfrc4hFXUAqcWNtCu6E7BL7v5EitZdk4uUJekg2")
		if err != nil {
			fmt.Printf("error %v\n", err)
		}
		targetA, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/9000/quic")
		host.Peerstore().AddAddr(targetID, targetA, time.Hour)
		dhtc.Connect(targetID)
	*/
	// Now sit and wait...

}
