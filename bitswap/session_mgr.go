package bitswap

import (
	"fmt"

	pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/ipfs/go-cid"
	outputdata "github.com/jimmyaxod/ipfscrawl/data"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	blocks "github.com/ipfs/go-block-format"
	merkeldag "github.com/ipfs/go-merkledag"
)

var (
	p_read = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_read", Help: ""})
	p_read_links = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_read_links", Help: ""})
	p_written = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_written", Help: ""})
)

type BitswapSessionMgr struct {
	log_outgoing         outputdata.Outputdata
	log_incoming         outputdata.Outputdata
	log_incoming_payload outputdata.Outputdata
	log_incoming_data    outputdata.Outputdata
	log_incoming_links   outputdata.Outputdata
}

func NewBitswapSessionMgr() *BitswapSessionMgr {
	output_file_period := int64(60 * 60)

	return &BitswapSessionMgr{
		log_outgoing:         outputdata.NewOutputdata("bitswap_out", output_file_period),
		log_incoming:         outputdata.NewOutputdata("bitswap_in", output_file_period),
		log_incoming_payload: outputdata.NewOutputdata("bitswap_in_payload", output_file_period),
		log_incoming_data:    outputdata.NewOutputdata("bitswap_in_data", output_file_period),
		log_incoming_links:   outputdata.NewOutputdata("bitswap_in_links", output_file_period),
	}
}

// Register a read happened
func (mgr *BitswapSessionMgr) RegisterRead(localPeerID string, peerID string, msg pb.Message) {
	p_read.Inc()

	numBlocks := 0
	if msg.Blocks != nil {
		numBlocks = len(msg.Blocks)
	}

	if msg.Payload != nil {
		// Log these separately
		for _, b := range msg.Payload {
			pref, err := cid.PrefixFromBytes(b.GetPrefix())
			if err != nil {
				return
			}

			c, err := pref.Sum(b.GetData())
			if err != nil {
				return
			}

			s := fmt.Sprintf("%s,%x,%s,%d", peerID, b.GetPrefix(), c, len(b.GetData()))
			mgr.log_incoming_payload.WriteData(s)

			bl := blocks.NewBlock(b.GetData())
			// Decode it...
			msg, err := merkeldag.DecodeProtobufBlock(bl)
			if err != nil {
				return
			}

			size, err := msg.Size()
			if err != nil {
				return
			}
			s = fmt.Sprintf("%s,%d", msg.Cid(), size)
			mgr.log_incoming_data.WriteData(s)

			for _, l := range msg.Links() {
				s := fmt.Sprintf("%s,%s,%d,%s", msg.Cid(), l.Name, l.Size, l.Cid)
				mgr.log_incoming_links.WriteData(s)
			}
		}
	} else {
		s := fmt.Sprintf("%s,%d,%d,%t", peerID, len(msg.Wantlist.Entries), numBlocks, msg.Payload == nil)
		mgr.log_incoming.WriteData(s)
	}

}

// Register a write happened
func (mgr *BitswapSessionMgr) RegisterWritten(localPeerID string, peerID string, msg pb.Message) {
	p_written.Inc()

	numBlocks := 0
	if msg.Blocks != nil {
		numBlocks = len(msg.Blocks)
	}

	s := fmt.Sprintf("%s,%d,%d,%t", peerID, len(msg.Wantlist.Entries), numBlocks, msg.Payload == nil)
	mgr.log_outgoing.WriteData(s)
}
