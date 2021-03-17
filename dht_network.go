package main

import (
	"context"
	"fmt"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	secio "github.com/libp2p/go-libp2p-secio"
	libp2ptls "github.com/libp2p/go-libp2p-tls"
)

// NullValidator is a validator that does no valiadtion
type NullValidator struct{}

// Validate always returns success
func (nv NullValidator) Validate(key string, value []byte) error {
	fmt.Printf("NullValidator Validate: %s - %s\n", key, string(value))
	return nil
}

// Select always selects the first record
func (nv NullValidator) Select(key string, values [][]byte) (int, error) {
	strs := make([]string, len(values))
	for i := 0; i < len(values); i++ {
		strs[i] = string(values[i])
	}
	fmt.Printf("NullValidator Select: %s - %v\n", key, strs)

	return 0, nil
}

type NetworkConn struct {
	DHT  *dht.IpfsDHT
	Host host.Host
}

// NewDHTNetwork creates a dht network...
func NewNetworkConn(cr int) NetworkConn {
	ctx := context.TODO()

	networkConn := NetworkConn{}

	priv, _, err := crypto.GenerateKeyPair(
		crypto.RSA, // Select your key type. Ed25519 are nice short
		2048,       // Select key length when possible (i.e. RSA).
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Created private key %s\n", priv)

	// Create a new host...
	networkConn.Host, err = libp2p.New(ctx,
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", 9000+cr),      // regular tcp connections
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", 9000+cr), // a UDP endpoint for the QUIC transport
		),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.Security(secio.ID, secio.New),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.DefaultTransports,
	)
	if err != nil {
		panic("Can't create host")
	}

	fmt.Printf("Created host %v\n", networkConn.Host)

	addrs := dht.GetDefaultBootstrapPeerAddrInfos()

	fmt.Printf("Using bootstrap servers %v\n", addrs)

	// Now create a kad dht
	networkConn.DHT, err = dht.New(ctx, networkConn.Host,
		dht.Mode(dht.ModeServer),
		dht.Concurrency(10),
		dht.BootstrapPeers(addrs...),
		dht.RoutingTableRefreshPeriod(10*time.Second),
		dht.RoutingTableRefreshQueryTimeout(30*time.Second),
		//dht.Validator(NullValidator{}),
	)
	if err != nil {
		panic("Can't create dht")
	}

	fmt.Printf("DHT created\n")

	err = networkConn.DHT.Bootstrap(ctx)
	if err != nil {
		panic("Can't bootstrap dht")
	}

	fmt.Printf("Bootstrapped\n")

	//networkConn.DHT.RefreshRoutingTable()

	//fmt.Printf("Refreshed routing table\n")

	//networkConn.DHT.RoutingTable().Print()

	return networkConn
}
