package bitswap

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/ipfs/go-cid"
	"github.com/jimmyaxod/ipfscrawl/data"
	outputdata "github.com/jimmyaxod/ipfscrawl/data"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	blocks "github.com/ipfs/go-block-format"
	merkeldag "github.com/ipfs/go-merkledag"
)

const CONNECTION_MAX_TIME = 10 * time.Second
const PROTOCOL_BITSWAP = "/ipfs/bitswap/1.2.0"

var (
	p_read = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_read", Help: ""})
	p_read_links = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_read_links", Help: ""})
	p_written = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_written", Help: ""})

	p_con_bitswap_outgoing_fail = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_outgoing_fail", Help: ""})
	p_con_bitswap_outgoing_success = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_outgoing_success", Help: ""})

	p_con_bitswap_incoming = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_incoming", Help: ""})
	p_con_bitswap_incoming_rejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_incoming_rejected", Help: ""})
)

type BitswapSessionMgr struct {
	log_outgoing         outputdata.Outputdata
	log_incoming         outputdata.Outputdata
	log_incoming_payload outputdata.Outputdata
	log_incoming_data    outputdata.Outputdata
	log_incoming_links   outputdata.Outputdata
}

func NewBitswapSessionMgr(hosts []host.Host) *BitswapSessionMgr {
	output_file_period := int64(60 * 60)

	mgr := &BitswapSessionMgr{
		log_outgoing:         outputdata.NewOutputdata("bitswap_out", output_file_period),
		log_incoming:         outputdata.NewOutputdata("bitswap_in", output_file_period),
		log_incoming_payload: outputdata.NewOutputdata("bitswap_in_payload", output_file_period),
		log_incoming_data:    outputdata.NewOutputdata("bitswap_in_data", output_file_period),
		log_incoming_links:   outputdata.NewOutputdata("bitswap_in_links", output_file_period),
	}

	for _, host := range hosts {
		host.SetStreamHandler(PROTOCOL_BITSWAP, mgr.HandleNewStream)
	}

	return mgr
}

// Register a read happened
func (mgr *BitswapSessionMgr) RegisterRead(localPeerID string, peerID string, msg pb.Message) {
	// Don't really mind if something inside here panics...
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

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

			stats, err := msg.Stat()

			jsonData, err := json.Marshal(msg.Links())
			if err == nil {
				data.InsertContent(msg.Cid().String(), string(jsonData), stats.DataSize)
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

// handleNewStreamBitswap handles incoming streams
func (mgr *BitswapSessionMgr) HandleNewStream(s network.Stream) {
	p_con_bitswap_incoming.Inc()

	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	ses := NewBitswapSession(ctx, cancelFunc, mgr, s)
	go func() {
		ses.Handle()
		cancelFunc()
	}()
}

/**
 *
 *
 */
func (mgr *BitswapSessionMgr) SendBitswapRequest(host host.Host, id peer.ID, cid cid.Cid) error {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	s, err := host.NewStream(ctx, id, PROTOCOL_BITSWAP)
	if err != nil {
		p_con_bitswap_outgoing_fail.Inc()
		cancelFunc()
		return err
	}

	p_con_bitswap_outgoing_success.Inc()

	ses := NewBitswapSession(ctx, cancelFunc, mgr, s)
	go func() {
		msg := ses.MakeBitswapRequest(cid)
		ses.SendMsg(msg)
		// Don't need to do anything about the reply
		cancelFunc()
	}()

	return nil
}
