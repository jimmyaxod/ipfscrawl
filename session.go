package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/network"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-msgio"
)

type DHTSession struct {
	stream             network.Stream
	ReadChannel        chan pb.Message // Channel for incoming messages
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
func NewDHTSession(ctx context.Context, s network.Stream, isIncoming bool) DHTSession {
	session := DHTSession{
		ctime:             time.Now(),
		stream:            s,
		ReadChannel:       make(chan pb.Message, 1),
		context:           ctx,
		sid:               uuid.NewString(),
		total_messages_in: 0,
		SAVE_SESSION_LOGS: false,
		LOG_DATA_IN:       false,
		LOG_DATA_OUT:      false,
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
	return session
}

// Close this session do any tidying up etc
func (ses DHTSession) Close() {
	if !ses.SAVE_SESSION_LOGS {
		return
	}
	ses.Log(fmt.Sprintf("Total messages read %d", ses.total_messages_in))
	ses.logfile.Close()
}

// Log something for this session
func (ses DHTSession) Log(msg string) {
	if !ses.SAVE_SESSION_LOGS {
		return
	}
	tt := time.Since(ses.ctime).Seconds()
	ses.logfw.WriteString(fmt.Sprintf("%.2f: %s\n", tt, msg))
	ses.logfw.Flush()
}

// Write a message
func (ses DHTSession) Write(msg pb.Message) error {
	if ses.LOG_DATA_OUT {
		jsonBytes, _ := json.Marshal(msg)
		ses.Log(fmt.Sprintf(" -> %s", string(jsonBytes)))
	}
	data_ping, err := msg.Marshal()
	if err != nil {
		return err
	}
	err = ses.writer.WriteMsg(data_ping)
	if err != nil {
		return err
	}
	ses.total_messages_out++
	return nil
}

// Read messages and put them on the readChannel
func (ses DHTSession) readMessages() {
	r := msgio.NewVarintReaderSize(ses.stream, network.MessageSizeMax)

	for {
		// Check if the context is done...
		select {
		case <-ses.context.Done():
			close(ses.ReadChannel)
			return
		default:
		}

		var req pb.Message

		msgbytes, err := r.ReadMsg()
		if err != nil {
			close(ses.ReadChannel)
			return
		}

		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			close(ses.ReadChannel)
			return
		}
		ses.total_messages_in++
		if ses.LOG_DATA_IN {
			jsonBytes, _ := json.Marshal(req)
			ses.Log(fmt.Sprintf(" <- %s", string(jsonBytes)))
		}
		ses.ReadChannel <- req
	}
}
