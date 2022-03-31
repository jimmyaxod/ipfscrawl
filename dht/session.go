package dht

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"
)

/**
 * Represents a dht session between peers
 * Can optionally log all messages to session logs
 */

type DHTSession struct {
	mgr                *DHTSessionMgr
	stream             network.Stream
	isIncoming         bool
	readChannel        chan pb.Message // Channel for incoming messages
	context            context.Context
	sid                string
	logfile            *os.File
	logfw              *bufio.Writer
	ctime              time.Time
	total_messages_in  int
	total_messages_out int
	writer             msgio.WriteCloser
	SAVE_SESSION_LOGS  bool
	LOG_DATA_IN        bool
	LOG_DATA_OUT       bool
}

// Create a new DHTSession
func NewDHTSession(ctx context.Context, mgr *DHTSessionMgr, s network.Stream, isIncoming bool) *DHTSession {
	session := DHTSession{
		mgr:               mgr,
		ctime:             time.Now(),
		stream:            s,
		isIncoming:        isIncoming,
		readChannel:       make(chan pb.Message, 1),
		context:           ctx,
		sid:               uuid.NewString(),
		total_messages_in: 0,
		SAVE_SESSION_LOGS: true,
		LOG_DATA_IN:       true,
		LOG_DATA_OUT:      true,
	}

	if session.SAVE_SESSION_LOGS {
		pid := s.Conn().RemotePeer()

		// Create a session log
		direction := "out"
		if isIncoming {
			direction = "in"
		}

		f, err := os.Create(fmt.Sprintf("sessions/%s_%s_%s", direction, pid, session.sid))
		if err != nil {
			panic("Error creating session log!")
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
	if !ses.SAVE_SESSION_LOGS {
		return
	}
	ses.Log(fmt.Sprintf("Total messages read %d written %d", ses.total_messages_in, ses.total_messages_out))
	ses.logfile.Close()
}

// Log something for this session
func (ses *DHTSession) Log(msg string) {
	if !ses.SAVE_SESSION_LOGS {
		return
	}
	tt := time.Since(ses.ctime).Seconds()
	ses.logfw.WriteString(fmt.Sprintf("%.2f: %s\n", tt, msg))
	ses.logfw.Flush()
}

// Write a message to the other peer
func (ses *DHTSession) Write(msg pb.Message) error {
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
	ses.mgr.RegisterWritten(msg)
	ses.total_messages_out++
	return nil
}

// Read messages and put them on the readChannel
func (ses *DHTSession) readMessages() {
	r := msgio.NewVarintReaderSize(ses.stream, network.MessageSizeMax)

	for {
		// Check if the context is done...
		select {
		case <-ses.context.Done():
			close(ses.readChannel)
			return
		default:
		}

		var req pb.Message

		msgbytes, err := r.ReadMsg()
		if err != nil {
			close(ses.readChannel)
			return
		}

		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			close(ses.readChannel)
			return
		}
		ses.mgr.RegisterRead(req)
		ses.total_messages_in++
		if ses.LOG_DATA_IN {
			jsonBytes, _ := json.Marshal(req)
			ses.Log(fmt.Sprintf(" <- %s", string(jsonBytes)))
		}
		ses.readChannel <- req
	}
}

// handleSession handles a new RPC...
func (ses *DHTSession) Handle() {

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

	if !ses.isIncoming {
		// Do a FIND_NODE
		key := make([]byte, 16)
		rand.Read(key)

		msg := pb.Message{
			Type: pb.Message_FIND_NODE,
			Key:  key,
		}
		err := ses.Write(msg)
		if err != nil {
			return
		}

		ses.Log(fmt.Sprintf("writer sent find_node"))

		select {
		case <-ses.context.Done():
			return
		case req, ok := <-ses.readChannel:
			if !ok {
				// Error on read, let's close things up...
				return
			}

			if req.GetType() == pb.Message_FIND_NODE {
				if len(req.CloserPeers) > 0 {
					ses.Log(fmt.Sprintf("reader CloserPeers %d", len(req.CloserPeers)))
					// Check out the FIND_NODE on that!
					for _, cpeer := range req.CloserPeers {

						pid, err := peer.IDFromBytes([]byte(cpeer.Id))
						if err == nil {
							for _, a := range cpeer.Addrs {
								ad, err := multiaddr.NewMultiaddrBytes(a)
								if err == nil && isConnectable(ad) {

									ses.mgr.NotifyCloserPeers(localPeerID, peerID, pid, ad)
								}
							}
						}
					}
				}

			} else {
				ses.Log(fmt.Sprintf("Unexpected message"))
			}
		}

	} else {
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
				rec := req.GetRecord()
				ses.mgr.NotifyPutValue(localPeerID, peerID, req.GetKey(), rec)
				// TODO: Any reply?
			case pb.Message_GET_VALUE:
				ses.Log(fmt.Sprintf("reader get_value"))

				ses.mgr.NotifyGetValue(localPeerID, peerID, req.GetKey())
				// TODO: Reply
			case pb.Message_ADD_PROVIDER:
				ses.Log(fmt.Sprintf("reader add_provider"))
				pinfos := pb.PBPeersToPeerInfos(req.GetProviderPeers())
				_, cid, err := cid.CidFromBytes(req.GetKey())
				if err == nil {
					ses.mgr.NotifyAddProvider(localPeerID, peerID, cid, pinfos)
				}
				// TODO: Reply?
			case pb.Message_GET_PROVIDERS:
				ses.Log(fmt.Sprintf("reader get_provider"))

				_, cid, err := cid.CidFromBytes(req.GetKey())
				if err == nil {
					ses.mgr.NotifyGetProviders(localPeerID, peerID, cid)
				}
			}
		}
	}
}
