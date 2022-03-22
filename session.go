package main

import (
	"context"

	"github.com/libp2p/go-libp2p-core/network"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-msgio"
)

type DHTSession struct {
	stream      network.Stream
	ReadChannel chan pb.Message // Channel for incoming messages
	context     context.Context
}

// Create a new DHTSession
func NewDHTSession(ctx context.Context, s network.Stream, isIncoming bool) DHTSession {
	session := DHTSession{
		stream:      s,
		ReadChannel: make(chan pb.Message, 1),
		context:     ctx,
	}

	// Read messages and put them on the ReadChannel
	go session.readMessages()
	return session
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
		ses.ReadChannel <- req
	}
}
