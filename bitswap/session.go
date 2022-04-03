package bitswap

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	outputdata "github.com/jimmyaxod/ipfscrawl/data"

	"github.com/google/uuid"
	pb "github.com/ipfs/go-bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio"

	merkeldag "github.com/ipfs/go-merkledag"
)

/**
 * Represents a bitswap session between peers
 * Can optionally log all messages to session logs
 */

type BitswapSession struct {
	stream             network.Stream
	readChannel        chan pb.Message // Channel for any incoming messages
	context            context.Context
	sid                string
	logfile            *os.File
	logfw              *bufio.Writer
	ctime              time.Time
	total_messages_in  int
	total_messages_out int
	writer             msgio.WriteCloser

	SAVE_SESSION_LOGS bool
	LOG_DATA_IN       bool
	LOG_DATA_OUT      bool

	log_incoming         outputdata.Outputdata
	log_incoming_payload outputdata.Outputdata
	log_incoming_data    outputdata.Outputdata
	log_incoming_links   outputdata.Outputdata
}

// Create a new DHTSession
func NewBitswapSession(ctx context.Context, s network.Stream) *BitswapSession {
	output_file_period := int64(60 * 60)

	session := BitswapSession{
		ctime:                time.Now(),
		stream:               s,
		readChannel:          make(chan pb.Message, 1),
		context:              ctx,
		sid:                  uuid.NewString(),
		total_messages_in:    0,
		total_messages_out:   0,
		SAVE_SESSION_LOGS:    false,
		LOG_DATA_IN:          false,
		LOG_DATA_OUT:         false,
		log_incoming:         outputdata.NewOutputdata("bitswap_in", output_file_period),
		log_incoming_payload: outputdata.NewOutputdata("bitswap_in_payload", output_file_period),
		log_incoming_data:    outputdata.NewOutputdata("bitswap_in_data", output_file_period),
		log_incoming_links:   outputdata.NewOutputdata("bitswap_in_links", output_file_period),
	}

	// Create a session log if required
	if session.SAVE_SESSION_LOGS {
		pid := s.Conn().RemotePeer()
		direction := s.Stat().Direction.String()

		f, err := os.Create(fmt.Sprintf("sessions/bitswap_%s_%s_%s", direction, pid, session.sid))
		if err != nil {
			panic("Error creating session log! Out of disk space? sessions dir exist?")
		}
		session.logfw = bufio.NewWriter(f)
		session.logfile = f
	}

	session.writer = msgio.NewVarintWriter(s)

	// Read messages and put them on the ReadChannel
	go session.readMessages()
	return &session
}

// Close this session do any tidying up etc
func (ses *BitswapSession) Close() {
	if ses.SAVE_SESSION_LOGS {
		ses.Log(fmt.Sprintf("Total messages read %d written %d", ses.total_messages_in, ses.total_messages_out))
		ses.logfile.Close()
	}

	// TODO: Close stream? So reader quits etc
}

// Log something for this session
func (ses *BitswapSession) Log(msg string) {
	if ses.SAVE_SESSION_LOGS {
		tt := time.Since(ses.ctime).Seconds()
		ses.logfw.WriteString(fmt.Sprintf("%.2f: %s\n", tt, msg))
		ses.logfw.Flush()
	}
}

// Write a message to the other peer
func (ses *BitswapSession) Write(msg pb.Message) error {
	//localPeerID := ses.stream.Conn().LocalPeer().Pretty()
	//peerID := ses.stream.Conn().RemotePeer().Pretty()

	if ses.LOG_DATA_OUT {
		jsonBytes, _ := json.Marshal(msg)
		ses.Log(fmt.Sprintf(" -> %s", string(jsonBytes)))
	}
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	err = ses.writer.WriteMsg(data)
	if err != nil {
		return err
	}
	//ses.mgr.RegisterWritten(localPeerID, peerID, msg)
	ses.total_messages_out++
	return nil
}

// Read messages and put them on the readChannel
func (ses *BitswapSession) readMessages() {
	r := msgio.NewVarintReaderSize(ses.stream, network.MessageSizeMax)
	defer close(ses.readChannel)

	//localPeerID := ses.stream.Conn().LocalPeer().Pretty()
	//peerID := ses.stream.Conn().RemotePeer().Pretty()

	for {
		// Check if the context is done...
		select {
		case <-ses.context.Done():
			return
		default:
		}

		var req pb.Message

		msgbytes, err := r.ReadMsg()
		if err != nil {
			return
		}

		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			return
		}
		//ses.mgr.RegisterRead(localPeerID, peerID, req)
		ses.total_messages_in++
		if ses.LOG_DATA_IN {
			jsonBytes, _ := json.Marshal(req)
			ses.Log(fmt.Sprintf(" <- %s", string(jsonBytes)))
		}
		ses.readChannel <- req
	}
}

// Sends a msg and waits for a response, then tidy up.
func (ses *BitswapSession) SendMsg(msg pb.Message) (pb.Message, error) {
	peerID := ses.stream.Conn().RemotePeer().Pretty()
	localPeerID := ses.stream.Conn().LocalPeer().Pretty()
	ses.Log(fmt.Sprintf("SendMsg %s -> %s", localPeerID, peerID))

	// cancel context, and drain the read channel so it's not blocked and can exit thread etc
	defer func() {
		ses.stream.Close()
		ses.stream.Conn().Close()

		for {
			_, ok := <-ses.readChannel
			if !ok {
				break
			}
		}
	}()

	err := ses.Write(msg)
	if err != nil {
		return pb.Message{}, err
	}

	ses.Log(fmt.Sprintf("writer sent %s", msg.String()))

	select {
	case <-ses.context.Done():
		return pb.Message{}, errors.New("Context done")
	case resp, ok := <-ses.readChannel:
		if !ok {
			// Error on read, let's close things up...
			return pb.Message{}, errors.New("Error on read")
		}
		// Return the resp
		return resp, nil
	}
}

// handleSession handles a new incoming stream
func (ses *BitswapSession) Handle() {

	peerID := ses.stream.Conn().RemotePeer().Pretty()
	localPeerID := ses.stream.Conn().LocalPeer().Pretty()
	ses.Log(fmt.Sprintf("Start %s -> %s", localPeerID, peerID))

	// cancel context, and drain the read channel so it's not blocked and can exit thread etc
	defer func() {
		ses.stream.Close()
		ses.stream.Conn().Close()

		for {
			_, ok := <-ses.readChannel
			if !ok {
				break
			}
		}
	}()

	// Incoming request/answer/etc
	for {
		select {
		case <-ses.context.Done():
			return
		case req, ok := <-ses.readChannel:
			if !ok {
				// Error on read, let's close things up...
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

					s := fmt.Sprintf("%s,%x,%s,%d", peerID, b.GetPrefix(), c, len(b.GetData()))
					ses.log_incoming_payload.WriteData(s)

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
					ses.log_incoming_data.WriteData(s)

					for _, l := range msg.Links() {
						s := fmt.Sprintf("%s,%s,%d,%s", msg.Cid(), l.Name, l.Size, l.Cid)
						ses.log_incoming_links.WriteData(s)
					}
				}
			} else {
				s := fmt.Sprintf("%s,%d,%d,%t", peerID, len(req.Wantlist.Entries), numBlocks, req.Payload == nil)
				ses.log_incoming.WriteData(s)
			}

		}
	}
}
