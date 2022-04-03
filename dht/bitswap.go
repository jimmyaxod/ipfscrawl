package dht

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	outputdata "github.com/jimmyaxod/ipfscrawl/data"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	bs_pb "github.com/ipfs/go-bitswap/message/pb"
	merkeldag "github.com/ipfs/go-merkledag"
)

const (
	protocol_bitswap = "/ipfs/bitswap/1.2.0"
)

var (
	p_con_bitswap_outgoing_fail = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_con_outgoing_fail", Help: ""})
	p_con_bitswap_outgoing_success = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bitswap_con_outgoing_success", Help: ""})
)

type Bitswapper struct {
	hosts                []host.Host
	log_outgoing         outputdata.Outputdata
	log_incoming         outputdata.Outputdata
	log_incoming_payload outputdata.Outputdata
	log_incoming_data    outputdata.Outputdata
	log_incoming_links   outputdata.Outputdata
}

// NewDHT creates a new DHT on top of the given hosts
func NewBitswapper(hosts []host.Host) *Bitswapper {
	output_file_period := int64(60 * 60)

	swapper := &Bitswapper{
		hosts:                hosts,
		log_outgoing:         outputdata.NewOutputdata("bitswap_out", output_file_period),
		log_incoming:         outputdata.NewOutputdata("bitswap_in", output_file_period),
		log_incoming_payload: outputdata.NewOutputdata("bitswap_in_payload", output_file_period),
		log_incoming_data:    outputdata.NewOutputdata("bitswap_in_data", output_file_period),
		log_incoming_links:   outputdata.NewOutputdata("bitswap_in_links", output_file_period),
	}

	// Set it up to handle incoming streams of the correct protocol
	/*
		for _, host := range hosts {
			host.SetStreamHandler(protocol_bitswap, swapper.handleNewStream)
		}
	*/
	return swapper
}

// handleNewStream
func (swapper *Bitswapper) handleNewStream(s network.Stream) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Panic occurred... we recovered...%v\n", err)
		}
	}()

	// TODO: Time limit context

	defer func() {
		s.Close()
		//		s.Conn().Close()
	}()

	// Do some reads...
	// Read any replies...
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	var req bs_pb.Message

	for {
		msgbytes, err := r.ReadMsg()
		if err != nil {
			return
		}

		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			return
		}

		numBlocks := 0
		if req.Blocks != nil {
			numBlocks = len(req.Blocks)
		}

		if req.Payload != nil {
			// Log these separately
			for _, b := range req.Payload {
				pref, err := cid.PrefixFromBytes(b.GetPrefix())
				if err != nil {
					return
				}

				c, err := pref.Sum(b.GetData())
				if err != nil {
					return
				}

				s := fmt.Sprintf("%s,%x,%s,%d", s.Conn().RemotePeer().Pretty(), b.GetPrefix(), c, len(b.GetData()))
				swapper.log_incoming_payload.WriteData(s)

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
				swapper.log_incoming_data.WriteData(s)

				for _, l := range msg.Links() {
					s := fmt.Sprintf("%s,%s,%d,%s", msg.Cid(), l.Name, l.Size, l.Cid)
					swapper.log_incoming_links.WriteData(s)

				}
			}
		} else {
			s := fmt.Sprintf("%s,%d,%d,%t", s.Conn().RemotePeer().Pretty(), len(req.Wantlist.Entries), numBlocks, req.Payload == nil)
			swapper.log_incoming.WriteData(s)
		}
	}
}

// Get a block from a peer
func (swapper *Bitswapper) Get(id peer.ID, cid cid.Cid) error {

	// Pick a host at random...
	host := swapper.hosts[rand.Intn(len(swapper.hosts))]

	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	s, err := host.NewStream(ctx, id, protocol_bitswap)
	if err != nil {
		p_con_bitswap_outgoing_fail.Inc()
		cancelFunc()
		return err
	}

	defer func() {
		cancelFunc()
		s.Close()
		//		s.Conn().Close()
	}()

	p_con_bitswap_outgoing_success.Inc()

	entries := make([]bs_pb.Message_Wantlist_Entry, 0)
	// Ask for a specific cid...
	entries = append(entries, bs_pb.Message_Wantlist_Entry{
		Block:    bs_pb.Cid{Cid: cid},
		Priority: 2147483632,
		WantType: bs_pb.Message_Wantlist_Block,
	})

	w := bs_pb.Message{
		Wantlist: bs_pb.Message_Wantlist{
			Entries: entries,
		},
	}

	data, err := w.Marshal()
	if err != nil {
		return err
	}

	jsonBytes, _ := json.Marshal(w)

	// Now we need to send 'data' on the wire...
	writer := msgio.NewVarintWriter(s)

	err = writer.WriteMsg(data)

	if err != nil {
		s := fmt.Sprintf("%s,%s", s.Conn().RemotePeer().Pretty(), string(jsonBytes))

		swapper.log_outgoing.WriteData(s)
	}

	return nil
}
