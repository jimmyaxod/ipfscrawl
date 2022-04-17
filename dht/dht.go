package dht

/**
 * DHT Crawler
 *
 *
 *
 */

import (
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/jimmyaxod/ipfscrawl/bitswap"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
)

const (
	protocol_dht = "/ipfs/kad/1.0.0"
)

const PERIOD_FIND_NODE = 200 * time.Millisecond
const MAX_NODE_DETAILS = 10000

type DHT struct {
	sessionMgr        *DHTSessionMgr
	sessionMgrBitswap *bitswap.BitswapSessionMgr
	nodedetails       NodeDetails
	target_peerstore  int
	Peerstore         peerstore.Peerstore
	hosts             []host.Host
}

// NewDHT creates a new DHT on top of the given hosts
func NewDHT(peerstore peerstore.Peerstore, hosts []host.Host) *DHT {
	dht := &DHT{
		nodedetails: *NewNodeDetails(MAX_NODE_DETAILS, peerstore),
		hosts:       hosts,
		Peerstore:   peerstore,
	}

	dht.sessionMgr = NewDHTSessionMgr(dht, &dht.nodedetails)
	dht.sessionMgrBitswap = bitswap.NewBitswapSessionMgr(hosts)

	// Handle incoming dht and bitswap protocols
	for _, host := range hosts {
		host.SetStreamHandler(protocol_dht, dht.sessionMgr.HandleNewStream)
	}

	// Start something to keep us alive with outgoing find_node calls
	go func() {
		find_node_ticker := time.NewTicker(PERIOD_FIND_NODE)

		for {
			select {
			case <-find_node_ticker.C:
				// Find something to connect to
				targetID := dht.nodedetails.Get()
				host := dht.hosts[rand.Intn(len(dht.hosts))]
				go dht.sessionMgr.SendRandomFindNode(host, targetID)
			}
		}
	}()

	return dht
}

func (dht *DHT) SendRandomFindNode(id peer.ID) error {
	host := dht.hosts[rand.Intn(len(dht.hosts))]
	return dht.sessionMgr.SendRandomFindNode(host, id)
}

func (dht *DHT) SendBitswapRequest(id peer.ID, cid cid.Cid) error {
	host := dht.hosts[rand.Intn(len(dht.hosts))]
	return dht.sessionMgrBitswap.SendBitswapRequest(host, id, cid)
}
