package dht

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/network"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-msgio"
)

/**
 * Represents a dht session between peers
 * Can optionally log all messages to session logs
 */

type DHTSession struct {
	mgr                *DHTSessionMgr
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
func NewDHTSession(ctx context.Context, cancelFunc func(), mgr *DHTSessionMgr, s network.Stream) *DHTSession {
	session := DHTSession{
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

		f, err := os.Create(fmt.Sprintf("sessions/%s_%s_%s", direction, pid, session.sid))
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
func (ses *DHTSession) Close() {
	if ses.SAVE_SESSION_LOGS {
		ses.Log(fmt.Sprintf("Total messages read %d written %d", ses.total_messages_in, ses.total_messages_out))
		ses.logfile.Close()
	}

	// TODO: Close stream? So reader quits etc
}

// Log something for this session
func (ses *DHTSession) Log(msg string) {
	if ses.SAVE_SESSION_LOGS {
		tt := time.Since(ses.ctime).Seconds()
		ses.logfw.WriteString(fmt.Sprintf("%.2f: %s\n", tt, msg))
		ses.logfw.Flush()
	}
}

// Write a message to the other peer
func (ses *DHTSession) Write(msg pb.Message) error {
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
func (ses *DHTSession) readMessages() {
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
func (ses *DHTSession) SendMsg(msg pb.Message) (pb.Message, error) {
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

	ses.Log(fmt.Sprintf("writer sent %s", msg.Type.String()))

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
func (ses *DHTSession) Handle() {

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

	// Incoming request
	select {
	case <-ses.context.Done():
		return
	case req, ok := <-ses.readChannel:
		if !ok {
			// Error on read, let's close things up...
			return
		}

		switch req.GetType() {
		case pb.Message_PING:
			ses.Log(fmt.Sprintf("reader ping"))
			// We need to reply to them with a ping then...
			msg_ping := pb.Message{
				Type:            pb.Message_PING,
				ClusterLevelRaw: req.ClusterLevelRaw,
			}

			err := ses.Write(msg_ping)
			if err != nil {
				return
			}
			ses.Log(fmt.Sprintf("writer sent ping reply"))
		case pb.Message_FIND_NODE:
			ses.Log(fmt.Sprintf("reader find_node"))
			// Make a reply and send it...

			// TODO: Fill in CloserPeers...
			msg := pb.Message{
				Type:            pb.Message_FIND_NODE,
				Key:             req.Key,
				CloserPeers:     make([]pb.Message_Peer, 0),
				ClusterLevelRaw: req.ClusterLevelRaw,
			}
			err := ses.Write(msg)
			if err != nil {
				return
			}

			ses.Log(fmt.Sprintf("writer sent find_node_reply"))
		case pb.Message_PUT_VALUE:
			ses.Log(fmt.Sprintf("reader put_value"))
			// TODO: Any reply?
		case pb.Message_GET_VALUE:
			ses.Log(fmt.Sprintf("reader get_value"))
			// TODO: Reply
		case pb.Message_ADD_PROVIDER:
			ses.Log(fmt.Sprintf("reader add_provider"))
			// TODO: Reply?
		case pb.Message_GET_PROVIDERS:
			ses.Log(fmt.Sprintf("reader get_provider"))
			// TODO: Reply?
		}
	}

}

func (ses *DHTSession) MakeRandomFindNode() pb.Message {
	// Do a FIND_NODE
	key := make([]byte, 16)
	rand.Read(key)

	msg := pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  key,
	}
	return msg
}
