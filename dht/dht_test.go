package dhtc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	secio "github.com/libp2p/go-libp2p-secio"
	libp2ptls "github.com/libp2p/go-libp2p-tls"
	"github.com/multiformats/go-multiaddr"
)

func TestDHTC(t *testing.T) {

	ctx := context.TODO()

	priv, _, err := crypto.GenerateKeyPair(
		crypto.RSA, // Select your key type. Ed25519 are nice short
		2048,       // Select key length when possible (i.e. RSA).
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Created private key\n")

	// Create a new host...
	host, err := libp2p.New(ctx,
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", 7000),      // regular tcp connections
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", 7000), // a UDP endpoint for the QUIC transport
		),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.Security(secio.ID, secio.New),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.DefaultTransports,
	)
	if err != nil {
		panic("Can't create host")
	}

	fmt.Printf("Created host %v\n", host)

	// Create a dht

	dhtc := NewDHT(host)
	/*
		addrs := dht.GetDefaultBootstrapPeerAddrInfos()
		// Put them in the peerstore...
		for _, addr := range addrs {
			for _, a := range addr.Addrs {
				fmt.Printf("Bootstrap %s %s\n", addr.ID, a)
			}
			host.Peerstore().AddAddrs(addr.ID, addr.Addrs, time.Hour)
		}

		for _, addr := range addrs {
			dhtc.Connect(addr.ID)
		}
	*/

	targetID, err := peer.Decode("QmYHqPxxfrc4hFXUAqcWNtCu6E7BL7v5EitZdk4uUJekg2")
	if err != nil {
		fmt.Printf("error %v\n", err)
	}
	targetA, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/9000/quic")
	host.Peerstore().AddAddr(targetID, targetA, time.Hour)
	dhtc.Connect(targetID)

	// Now sit and wait...

	time.Sleep(24 * time.Hour)
}
