package bitswap

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	pb "github.com/ipfs/go-bitswap/message/pb"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-msgio"
)

/**
 * Represents a bitswap session between peers
 * Can optionally log all messages to session logs
 */

type BitswapSession struct {
	mgr                *BitswapSessionMgr
	stream             network.Stream
	readChannel        chan pb.Message // Channel for any incoming messages
	context            context.Context
	cancelFunc         func()
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
}

// Create a new DHTSession
func NewBitswapSession(ctx context.Context, cancelFunc func(), mgr *BitswapSessionMgr, s network.Stream) *BitswapSession {
	session := BitswapSession{
		mgr:                mgr,
		ctime:              time.Now(),
		stream:             s,
		readChannel:        make(chan pb.Message, 1),
		context:            ctx,
		cancelFunc:         cancelFunc,
		sid:                uuid.NewString(),
		total_messages_in:  0,
		total_messages_out: 0,
		SAVE_SESSION_LOGS:  false,
		LOG_DATA_IN:        false,
		LOG_DATA_OUT:       false,
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
	localPeerID := ses.stream.Conn().LocalPeer().Pretty()
	peerID := ses.stream.Conn().RemotePeer().Pretty()

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
	ses.mgr.RegisterWritten(localPeerID, peerID, msg)
	ses.total_messages_out++
	return nil
}

// Read messages and put them on the readChannel
func (ses *BitswapSession) readMessages() {
	r := msgio.NewVarintReaderSize(ses.stream, network.MessageSizeMax)
	defer func() {
		close(ses.readChannel)
		ses.cancelFunc()
	}()

	localPeerID := ses.stream.Conn().LocalPeer().Pretty()
	peerID := ses.stream.Conn().RemotePeer().Pretty()

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
		ses.mgr.RegisterRead(localPeerID, peerID, req)
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
		ses.cancelFunc()

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
		ses.cancelFunc()

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
		case _, ok := <-ses.readChannel:
			if !ok {
				// Error on read, let's close things up...
				return
			}
		}
	}
}
