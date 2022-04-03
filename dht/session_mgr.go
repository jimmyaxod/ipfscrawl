package dht

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	ipnspb "github.com/ipfs/go-ipns/pb"
	outputdata "github.com/jimmyaxod/ipfscrawl/data"
	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	record_pb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	p_read_ping = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_ping", Help: ""})
	p_read_find_node = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_find_node", Help: ""})
	p_read_add_provider = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_add_provider", Help: ""})
	p_read_get_providers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_get_providers", Help: ""})
	p_read_put_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_put_value", Help: ""})
	p_read_get_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_get_value", Help: ""})

	p_written_ping = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_ping", Help: ""})
	p_written_find_node = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_find_node", Help: ""})
	p_written_add_provider = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_add_provider", Help: ""})
	p_written_get_providers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_get_providers", Help: ""})
	p_written_put_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_put_value", Help: ""})
	p_written_get_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_get_value", Help: ""})

	p_peers_found = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_peers_found", Help: ""})
)

type DHTSessionMgr struct {
	dht         *DHT
	nodeDetails *NodeDetails
	swapper     *Bitswapper

	log_peerinfo     outputdata.Outputdata
	log_addproviders outputdata.Outputdata
	log_getproviders outputdata.Outputdata
	log_put          outputdata.Outputdata
	log_get          outputdata.Outputdata
	log_put_ipns     outputdata.Outputdata
	log_get_ipns     outputdata.Outputdata
	log_put_pk       outputdata.Outputdata
}

func NewDHTSessionMgr(dht *DHT, nd *NodeDetails, sw *Bitswapper) *DHTSessionMgr {
	output_file_period := int64(60 * 60)

	return &DHTSessionMgr{
		dht:              dht,
		swapper:          sw,
		nodeDetails:      nd,
		log_peerinfo:     outputdata.NewOutputdata("peerinfo", output_file_period),
		log_addproviders: outputdata.NewOutputdata("addproviders", output_file_period),
		log_getproviders: outputdata.NewOutputdata("getproviders", output_file_period),
		log_put:          outputdata.NewOutputdata("put", output_file_period),
		log_get:          outputdata.NewOutputdata("get", output_file_period),
		log_put_ipns:     outputdata.NewOutputdata("put_ipns", output_file_period),
		log_get_ipns:     outputdata.NewOutputdata("get_ipns", output_file_period),
		log_put_pk:       outputdata.NewOutputdata("put_pk", output_file_period),
	}
}

// Register a read happened
func (mgr *DHTSessionMgr) RegisterRead(localPeerID string, peerID string, msg pb.Message) {
	switch msg.Type {
	case pb.Message_PING:
		p_read_ping.Inc()
	case pb.Message_FIND_NODE:
		p_read_find_node.Inc()
	case pb.Message_ADD_PROVIDER:
		p_read_add_provider.Inc()

		pinfos := pb.PBPeersToPeerInfos(msg.GetProviderPeers())
		_, cid, err := cid.CidFromBytes(msg.GetKey())
		if err == nil {
			mgr.logAddProvider(localPeerID, peerID, cid, pinfos)
		}
	case pb.Message_GET_PROVIDERS:
		p_read_get_providers.Inc()

		_, cid, err := cid.CidFromBytes(msg.GetKey())
		if err == nil {
			mgr.logGetProviders(localPeerID, peerID, cid)
		}

	case pb.Message_PUT_VALUE:
		p_read_put_value.Inc()

		mgr.logPutValue(localPeerID, peerID, msg.GetKey(), msg.GetRecord())
	case pb.Message_GET_VALUE:
		p_read_get_value.Inc()

		mgr.logGetValue(localPeerID, peerID, msg.GetKey())
	}

	// Parse any CloserPeers from any message...
	if msg.CloserPeers != nil && len(msg.CloserPeers) > 0 {
		for _, cpeer := range msg.CloserPeers {
			pid, err := peer.IDFromBytes([]byte(cpeer.Id))
			if err == nil {
				p_peers_found.Inc()
				for _, a := range cpeer.Addrs {
					ad, err := multiaddr.NewMultiaddrBytes(a)
					if err == nil && isConnectable(ad) {
						mgr.nodeDetails.AddAddr(pid, ad)

						// localPeerID, fromPeerID, newPeerID, addr
						s := fmt.Sprintf("%s,%s,%s,%s", localPeerID, peerID, pid, ad)
						mgr.log_peerinfo.WriteData(s)
					}
				}
			}
		}
	}

}

// Register a write happened
func (mgr *DHTSessionMgr) RegisterWritten(localPeerID string, peerID string, msg pb.Message) {
	switch msg.Type {
	case pb.Message_PING:
		p_written_ping.Inc()
	case pb.Message_FIND_NODE:
		p_written_find_node.Inc()
	case pb.Message_ADD_PROVIDER:
		p_written_add_provider.Inc()
	case pb.Message_GET_PROVIDERS:
		p_written_get_providers.Inc()
	case pb.Message_PUT_VALUE:
		p_written_put_value.Inc()
	case pb.Message_GET_VALUE:
		p_written_get_value.Inc()
	}
}

func (mgr *DHTSessionMgr) logAddProvider(localPeerID string, peerID string, cid cid.Cid, pinfo []*peer.AddrInfo) {
	// Log all providers...
	for _, pi := range pinfo {
		for _, a := range pi.Addrs {
			s := fmt.Sprintf("%s,%s,%s,%s", peerID, cid, pi.ID, a)
			mgr.log_addproviders.WriteData(s)
		}
		// Try to do some bitswap shenanigans...
		//		mgr.swapper.Get(pi.ID, cid)

	}
}

func (mgr *DHTSessionMgr) logGetProviders(localPeerID string, peerID string, cid cid.Cid) {
	s := fmt.Sprintf("%s,%s", peerID, cid)
	mgr.log_getproviders.WriteData(s)
}

func (mgr *DHTSessionMgr) logGetValue(localPeerID string, peerID string, key []byte) {

	s := fmt.Sprintf("%s,%x", peerID, key)
	mgr.log_get.WriteData(s)

	if strings.HasPrefix(string(key), "/ipns/") {
		// Extract the PID...
		pidbytes := key[6:]
		pid, err := peer.IDFromBytes(pidbytes)
		if err == nil {
			s := fmt.Sprintf("%s,%s", peerID, pid.Pretty())
			mgr.log_get_ipns.WriteData(s)
		}
	}
}

func (mgr *DHTSessionMgr) logPutValue(localPeerID string, peerID string, key []byte, rec *record_pb.Record) {

	s := fmt.Sprintf("%s,%x,%x,%x,%s", peerID, key, rec.Key, rec.Value, rec.TimeReceived)
	mgr.log_put.WriteData(s)

	// TODO: /pk/ - maps Hash(pubkey) to pubkey

	if strings.HasPrefix(string(rec.Key), "/pk/") {
		// Extract the PID...
		pidbytes := rec.Key[4:]
		pid, err := peer.IDFromBytes(pidbytes)
		if err == nil {
			s := fmt.Sprintf("%s,%s,%x", peerID, pid.Pretty(), rec.Value)
			mgr.log_put_pk.WriteData(s)
		}
	}

	// If it's ipns...
	if strings.HasPrefix(string(rec.Key), "/ipns/") {
		// Extract the PID...
		pidbytes := rec.Key[6:]
		pid, err := peer.IDFromBytes(pidbytes)
		if err == nil {
			// ok so it's an ipns record, so lets examine the data...
			ipns_rec := ipnspb.IpnsEntry{}
			err = ipns_rec.Unmarshal(rec.Value)
			if err == nil {
				// Log everything...
				//pubkey := ipns_rec.GetPubKey()
				//validity := ipns_rec.GetValidity()
				//signature := ipns_rec.GetSignature()

				ttl := ipns_rec.GetTtl()
				seq := ipns_rec.GetSequence()
				value := ipns_rec.GetValue()

				s := fmt.Sprintf("%s,%s,%s,%d,%d", peerID, pid.Pretty(), string(value), ttl, seq)
				mgr.log_put_ipns.WriteData(s)

			} else {
				fmt.Printf("Error unmarshalling ipns %v\n", err)
			}
		}
	}

}
