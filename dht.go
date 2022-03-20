package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	mh "github.com/multiformats/go-multihash"

	ipnspb "github.com/ipfs/go-ipns/pb"
)

/**
 * Here we're going to make a bare bones kad dht impl to play with the DHT
 *
 * We traverse the network by sending out FIND_NODE for random IDs. We get responses
 * with closer peers. We then continue with them.
 *
 * As we crawl the network, we also start getting other messages (eg GET_PROVIDERS)
 */

const (
	proto = "/ipfs/kad/1.0.0"
)

const CONNECTION_MAX_TIME = 5 * time.Minute
const MAX_CONNECTIONS = 1200
const TARGET_CONNECTIONS = 1024
const MAX_NODE_DETAILS = 100000

var (
	p_pending_connects = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_pending_connects", Help: ""})

	p_con_outgoing_fail = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_con_outgoing_fail", Help: ""})
	p_con_outgoing_success = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_con_outgoing_success", Help: ""})
	p_con_outgoing_rejected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_con_outgoing_rejected", Help: ""})
	p_con_incoming = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_con_incoming", Help: ""})
	p_con_incoming_rejected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_con_incoming_rejected", Help: ""})
	p_written_ping = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_written_ping", Help: ""})
	p_written_find_node = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_written_find_node", Help: ""})

	p_read_put_value = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_read_put_value", Help: ""})
	p_read_get_value = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_read_get_value", Help: ""})
	p_read_add_provider = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_read_add_provider", Help: ""})
	p_read_get_provider = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_read_get_provider", Help: ""})
	p_read_find_node = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_read_find_node", Help: ""})
	p_read_ping = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_read_ping", Help: ""})
	p_peers_found = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_peers_found", Help: ""})
	p_active_writers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_active_writers", Help: ""})
	p_active_readers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_active_readers", Help: ""})

	p_max_connections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_max_connections", Help: ""})
	p_target_connections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_target_connections", Help: ""})

	p_ns_total_connections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_ns_total_connections", Help: ""})
	p_ns_total_streams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_ns_total_streams", Help: ""})
	p_ns_total_dht_streams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_ns_total_dht_streams", Help: ""})
	p_ns_total_in_dht_streams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_ns_total_in_dht_streams", Help: ""})
	p_ns_total_out_dht_streams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_ns_total_out_dht_streams", Help: ""})

	p_nd_total_nodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_nd_total_nodes", Help: ""})
	p_nd_total_ready = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_nd_total_ready", Help: ""})
	p_nd_total_expired = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_nd_total_expired", Help: ""})
	p_nd_total_connected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_nd_total_connected", Help: ""})
	p_nd_avg_since = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dht_nd_avg_since", Help: ""})
)

// Update prometheus stats
func (dht *DHT) UpdateStats() {
	fmt.Printf("Updating prom stats...\n")
	p_con_outgoing_fail.Set(float64(dht.metric_con_outgoing_fail))
	p_con_outgoing_success.Set(float64(dht.metric_con_outgoing_success))
	p_con_outgoing_rejected.Set(float64(dht.metric_con_outgoing_rejected))
	p_con_incoming.Set(float64(dht.metric_con_incoming))
	p_con_incoming_rejected.Set(float64(dht.metric_con_incoming_rejected))

	p_written_ping.Set(float64(dht.metric_written_ping))
	p_written_find_node.Set(float64(dht.metric_written_find_node))

	p_read_put_value.Set(float64(dht.metric_read_put_value))
	p_read_get_value.Set(float64(dht.metric_read_get_value))
	p_read_add_provider.Set(float64(dht.metric_read_add_provider))
	p_read_get_provider.Set(float64(dht.metric_read_get_provider))
	p_read_find_node.Set(float64(dht.metric_read_find_node))
	p_read_ping.Set(float64(dht.metric_read_ping))

	p_peers_found.Set(float64(dht.metric_peers_found))

	p_active_writers.Set(float64(dht.metric_active_writers))
	p_active_readers.Set(float64(dht.metric_active_readers))

	p_target_connections.Set(float64(dht.target_connections))
	p_max_connections.Set(float64(dht.max_connections))

	total_connections, total_streams, total_dht_streams, total_in_dht_streams, total_out_dht_streams := dht.CurrentStreams()
	p_ns_total_connections.Set(float64(total_connections))
	p_ns_total_streams.Set(float64(total_streams))
	p_ns_total_dht_streams.Set(float64(total_dht_streams))
	p_ns_total_in_dht_streams.Set(float64(total_in_dht_streams))
	p_ns_total_out_dht_streams.Set(float64(total_out_dht_streams))

	total_nodes, total_ready, total_expired, total_connected, avg_since := dht.nodedetails.GetStats()

	p_nd_total_nodes.Set(float64(total_nodes))
	p_nd_total_ready.Set(float64(total_ready))
	p_nd_total_expired.Set(float64(total_expired))
	p_nd_total_connected.Set(float64(total_connected))
	p_nd_avg_since.Set(float64(avg_since))

	p_pending_connects.Set(float64(dht.metric_pending_connect))
}

type DHT struct {
	nodedetails NodeDetails

	target_connections int
	max_connections    int
	target_peerstore   int

	hostmu    sync.Mutex
	peerstore peerstore.Peerstore
	hosts     []host.Host

	started time.Time

	metric_pending_connect       int64
	metric_con_outgoing_fail     uint64
	metric_con_outgoing_success  uint64
	metric_con_outgoing_rejected uint64
	metric_con_incoming          uint64
	metric_con_incoming_rejected uint64
	metric_written_ping          uint64
	metric_written_find_node     uint64
	metric_read_put_value        uint64
	metric_read_get_value        uint64
	metric_read_add_provider     uint64
	metric_read_get_provider     uint64
	metric_read_find_node        uint64
	metric_read_ping             uint64
	metric_peers_found           uint64

	metric_active_writers int64
	metric_active_readers int64

	log_peerinfo       Outputdata
	log_addproviders   Outputdata
	log_getproviders   Outputdata
	log_put            Outputdata
	log_get            Outputdata
	log_put_ipns       Outputdata
	log_get_ipns       Outputdata
	log_put_pk         Outputdata
	log_peer_protocols Outputdata
	log_peer_agents    Outputdata
	log_peer_ids       Outputdata
	log_sessions_r     Outputdata
	log_sessions_w     Outputdata
	log_stats          Outputdata
}

// NewDHT creates a new DHT on top of the given hosts
func NewDHT(peerstore peerstore.Peerstore, hosts []host.Host) *DHT {
	output_file_period := int64(60 * 60)

	dht := &DHT{
		nodedetails:        *NewNodeDetails(MAX_NODE_DETAILS),
		target_connections: TARGET_CONNECTIONS,
		max_connections:    MAX_CONNECTIONS,
		hosts:              make([]host.Host, 0),
		started:            time.Now(),
		log_peerinfo:       NewOutputdata("peerinfo", output_file_period),
		log_peer_protocols: NewOutputdata("peerprotocols", output_file_period),
		log_peer_agents:    NewOutputdata("peeragents", output_file_period),
		log_peer_ids:       NewOutputdata("peerids", output_file_period),
		log_addproviders:   NewOutputdata("addproviders", output_file_period),
		log_getproviders:   NewOutputdata("getproviders", output_file_period),
		log_put:            NewOutputdata("put", output_file_period),
		log_get:            NewOutputdata("get", output_file_period),
		log_put_ipns:       NewOutputdata("put_ipns", output_file_period),
		log_get_ipns:       NewOutputdata("get_ipns", output_file_period),
		log_put_pk:         NewOutputdata("put_pk", output_file_period),
		log_sessions_r:     NewOutputdata("sessions_r", output_file_period),
		log_sessions_w:     NewOutputdata("sessions_w", output_file_period),
		log_stats:          NewOutputdataSimple("stats", output_file_period),
	}

	// Start something to try keep us alive...
	go func() {
		for {
			total_dht_streams := int(atomic.LoadInt64(&dht.metric_active_readers))

			total_dht_streams += int(atomic.LoadInt64(&dht.metric_pending_connect))

			// Check if we should top us up...
			/*
				total_dht_streams := 0
				dht.hostmu.Lock()
				for _, host := range dht.hosts {
					cons := host.Network().Conns()
					for _, con := range cons {
						for _, stream := range con.GetStreams() {
							if stream.Protocol() == proto {
								total_dht_streams++
							}
						}
					}
				}
				dht.hostmu.Unlock()
			*/
			if total_dht_streams < dht.target_connections {
				// Find something to connect to
				p := dht.nodedetails.Get()
				if p != "" {
					targetID, err := peer.Decode(p)
					if err == nil {
						//fmt.Printf("Would try connect to %v\n", targetID)
						go dht.Connect(targetID)
					}
				}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	dht.SetHosts(peerstore, hosts)

	return dht
}

func (dht *DHT) SetHosts(peerstore peerstore.Peerstore, hosts []host.Host) {
	dht.hostmu.Lock()
	defer dht.hostmu.Unlock()

	// If we have existing hosts, close them all
	go func(oldhosts []host.Host) {
		fmt.Printf("Closing hosts [%d]\n", len(oldhosts))
		for i, host := range oldhosts {
			fmt.Printf("Closing host %d\n", i)
			host.Close()
		}
	}(dht.hosts)

	fmt.Printf("Setting up new hosts and peerstore\n")
	// Setup hosts + peerstore
	dht.peerstore = peerstore
	dht.hosts = hosts

	// Set it up to handle incoming streams of the correct protocol
	for _, host := range hosts {
		host.SetStreamHandler(proto, dht.handleNewStream)
	}
}

// CurrentStreams - get number of current streams
func (dht *DHT) CurrentStreams() (int, int, int, int, int) {
	dht.hostmu.Lock()
	defer dht.hostmu.Unlock()

	total_connections := 0
	total_streams := 0
	total_dht_streams := 0

	total_in_dht_streams := 0
	total_out_dht_streams := 0

	for _, host := range dht.hosts {
		cons := host.Network().Conns()

		total_connections += len(cons)
		for _, con := range cons {
			total_streams += len(con.GetStreams())
			for _, stream := range con.GetStreams() {
				if stream.Protocol() == proto {
					total_dht_streams++
					stats := stream.Stat()
					if stats.Direction == network.DirInbound {
						total_in_dht_streams++
					} else {
						total_out_dht_streams++
					}
				}
			}
		}
	}
	return total_connections, total_streams, total_dht_streams, total_in_dht_streams, total_out_dht_streams
}

// ShowStats - print out some stats about our crawl
func (dht *DHT) ShowStats() {
	// How many connections do we have?, how many streams?
	dht.hostmu.Lock()
	total_peerstore := dht.peerstore.Peers().Len()
	num_hosts := len(dht.hosts)
	dht.hostmu.Unlock()

	total_connections, total_streams, total_dht_streams, total_in_dht_streams, total_out_dht_streams := dht.CurrentStreams()

	fmt.Printf("DHT uptime=%.2fs total_peers_found=%d\n", time.Since(dht.started).Seconds(), dht.metric_peers_found)
	fmt.Printf("Current hosts=%d cons=%d streams=%d dht_streams=%d (%d in %d out) peerstore=%d\n",
		num_hosts,
		total_connections,
		total_streams,
		total_dht_streams,
		total_in_dht_streams,
		total_out_dht_streams,
		total_peerstore)
	fmt.Printf("Total Connections out=%d (%d fails) (%d rejected) in=%d (%d rejected)\n",
		dht.metric_con_outgoing_success,
		dht.metric_con_outgoing_fail,
		dht.metric_con_outgoing_rejected,
		dht.metric_con_incoming,
		dht.metric_con_incoming_rejected)
	fmt.Printf("Total Writes ping=%d find_node=%d\n", dht.metric_written_ping, dht.metric_written_find_node)

	metric_active_writers := atomic.LoadInt64(&dht.metric_active_writers)
	metric_active_readers := atomic.LoadInt64(&dht.metric_active_readers)

	fmt.Printf("Active writers=%d readers=%d\n", metric_active_writers, metric_active_readers)

	// Stats on incoming messages
	fmt.Printf("Reads put=%d get=%d addprov=%d getprov=%d find_node=%d ping=%d\n",
		dht.metric_read_put_value, dht.metric_read_get_value,
		dht.metric_read_add_provider, dht.metric_read_get_provider,
		dht.metric_read_find_node, dht.metric_read_ping)

	fmt.Printf(dht.nodedetails.Stats())

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Memory Alloc = %v MiB", m.Alloc/(1024*1024))
	fmt.Printf(" TotalAlloc = %v MiB", m.TotalAlloc/(1024*1024))
	fmt.Printf(" Sys = %v MiB", m.Sys/(1024*1024))
	fmt.Printf(" NumGC = %v\n", m.NumGC)

	fmt.Println()

	total_nodes, total_ready, total_expired, total_connected, avg_since := dht.nodedetails.GetStats()

	// Also write it to a stats file...
	// NumGC, Allo, Sys,
	s := fmt.Sprintf("%v,%v,%v, %d,%d,%d,%d,%d,%d, %d,%d,%d,%d,%.2f",
		m.NumGC, m.Alloc/(1024*1024), m.Sys/(1024*1024),
		total_connections, total_streams, total_dht_streams,
		total_in_dht_streams, total_out_dht_streams, total_peerstore,
		total_nodes, total_ready, total_expired, total_connected, avg_since,
	)

	dht.log_stats.WriteData(s)

}

// Connect connects to a new peer, and starts an eventloop for it
// Assumes that tryConnectTo has already been called...
func (dht *DHT) Connect(id peer.ID) error {
	//	fmt.Printf("Outgoing [%s]\n", id.Pretty())

	atomic.AddInt64(&dht.metric_pending_connect, 1)
	defer func() {
		atomic.AddInt64(&dht.metric_pending_connect, -1)
	}()

	dht.nodedetails.Add(id.Pretty())

	total_dht_streams := int(atomic.LoadInt64(&dht.metric_active_readers))

	//_, _, total_dht_streams, _, _ := dht.CurrentStreams()
	if total_dht_streams >= dht.target_connections {
		if total_dht_streams >= dht.max_connections {
			atomic.AddUint64(&dht.metric_con_outgoing_rejected, 1)
			return errors.New("No capacity")
		}
		if !dht.nodedetails.ReadyForConnect(id.Pretty()) {
			atomic.AddUint64(&dht.metric_con_outgoing_rejected, 1)
			return errors.New("Dupe")
		}
	}

	// Pick a host at random...
	dht.hostmu.Lock()
	host := dht.hosts[rand.Intn(len(dht.hosts))]
	dht.hostmu.Unlock()

	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)

	s, err := host.NewStream(ctx, id, proto)
	if err != nil {
		dht.nodedetails.ConnectFailure(id.Pretty())
		atomic.AddUint64(&dht.metric_con_outgoing_fail, 1)
		cancelFunc()
		return err
	}
	dht.nodedetails.Connected(id.Pretty())
	dht.nodedetails.ConnectSuccess(id.Pretty())
	atomic.AddUint64(&dht.metric_con_outgoing_success, 1)
	dht.ProcessPeerStream(ctx, cancelFunc, s)
	return nil
}

// handleNewStream handles incoming streams
func (dht *DHT) handleNewStream(s network.Stream) {
	pid := s.Conn().RemotePeer()

	//	fmt.Printf("Incoming [%s]\n", pid.Pretty())
	dht.nodedetails.Add(pid.Pretty())

	// If we have enough connections, check if we should reject it
	total_dht_streams := int(atomic.LoadInt64(&dht.metric_active_readers))

	//_, _, total_dht_streams, _, _ := dht.CurrentStreams()
	if total_dht_streams >= dht.target_connections {
		if total_dht_streams >= dht.max_connections {
			atomic.AddUint64(&dht.metric_con_incoming_rejected, 1)
			s.Close()
			s.Conn().Close()
			return
		}

		if !dht.nodedetails.ReadyForConnect(pid.Pretty()) {
			atomic.AddUint64(&dht.metric_con_incoming_rejected, 1)
			s.Close()
			s.Conn().Close()
			return
		}
	}

	// Handle it...
	dht.nodedetails.ConnectSuccess(pid.Pretty())
	dht.nodedetails.Connected(pid.Pretty())

	atomic.AddUint64(&dht.metric_con_incoming, 1)
	ctx, cancelFunc := context.WithTimeout(context.TODO(), CONNECTION_MAX_TIME)
	dht.ProcessPeerStream(ctx, cancelFunc, s)
}

// doPeriodicWrites handles sending periodic FIND_NODE and PING
func (dht *DHT) doPeriodicWrites(ctx context.Context, cancelFunc context.CancelFunc, s network.Stream) {
	peerID := s.Conn().RemotePeer().Pretty()
	localPeerID := s.Conn().LocalPeer().Pretty()

	atomic.AddInt64(&dht.metric_active_writers, 1)
	defer atomic.AddInt64(&dht.metric_active_writers, -1)

	total_msgs := 0
	ctime := time.Now()

	defer func() {
		etime := time.Now()
		total_duration := etime.Unix() - ctime.Unix()
		s := fmt.Sprintf("%s,%s,%s,%s,%d,%d", localPeerID, peerID, ctime, etime, total_duration, total_msgs)

		dht.log_sessions_w.WriteData(s)
	}()

	defer cancelFunc()

	// Close the stream in the writer only...
	defer func() {
		s.Close()
		s.Conn().Close()
	}()

	defer dht.nodedetails.Disconnected(peerID)

	w := msgio.NewVarintWriter(s)

	ticker_ping := time.NewTicker(30 * time.Second)
	ticker_find_node := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker_ping.C:
			// Send out a ping
			msg_ping := pb.Message{
				Type: pb.Message_PING,
			}

			data_ping, err := msg_ping.Marshal()
			if err != nil {
				return
			}
			err = w.WriteMsg(data_ping)
			if err != nil {
				return
			}
			total_msgs++
			atomic.AddUint64(&dht.metric_written_ping, 1)
		case <-ticker_find_node.C:
			// Send out a find_node for a random key

			key := make([]byte, 16)
			rand.Read(key)

			msg := pb.Message{
				Type: pb.Message_FIND_NODE,
				Key:  key,
			}

			data, err := msg.Marshal()
			if err != nil {
				return
			}
			err = w.WriteMsg(data)
			if err != nil {
				return
			}
			total_msgs++
			atomic.AddUint64(&dht.metric_written_find_node, 1)
		case <-ctx.Done():
			return
		}
	}
}

// doReading reads msgs from stream and processes...
func (dht *DHT) doReading(ctx context.Context, cancelFunc context.CancelFunc, s network.Stream) {
	peerID := s.Conn().RemotePeer().Pretty()
	localPeerID := s.Conn().LocalPeer().Pretty()

	total_msgs := 0
	ctime := time.Now()

	atomic.AddInt64(&dht.metric_active_readers, 1)
	defer atomic.AddInt64(&dht.metric_active_readers, -1)

	defer func() {
		etime := time.Now()
		total_duration := etime.Unix() - ctime.Unix()
		s := fmt.Sprintf("%s,%s,%s,%s,%d,%d", localPeerID, peerID, ctime, etime, total_duration, total_msgs)

		dht.log_sessions_r.WriteData(s)
	}()

	defer cancelFunc()

	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	for {
		// Check if the context is done...
		select {
		case <-ctx.Done():
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

		total_msgs++

		switch req.GetType() {
		case 0:
			atomic.AddUint64(&dht.metric_read_put_value, 1)

			rec := req.GetRecord()

			s := fmt.Sprintf("%s,%x,%x,%x,%s", peerID, req.GetKey(), rec.Key, rec.Value, rec.TimeReceived)
			dht.log_put.WriteData(s)

			// TODO: /pk/ - maps Hash(pubkey) to pubkey

			if strings.HasPrefix(string(rec.Key), "/pk/") {
				// Extract the PID...
				pidbytes := rec.Key[4:]
				pid, err := peer.IDFromBytes(pidbytes)
				if err == nil {
					s := fmt.Sprintf("%s,%s,%x", peerID, pid.Pretty(), rec.Value)
					dht.log_put_pk.WriteData(s)
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
						dht.log_put_ipns.WriteData(s)

					} else {
						fmt.Printf("Error unmarshalling ipns %v\n", err)
					}
				}
			}

		case 1:
			atomic.AddUint64(&dht.metric_read_get_value, 1)

			s := fmt.Sprintf("%s,%x", peerID, req.GetKey())
			dht.log_get.WriteData(s)

			if strings.HasPrefix(string(req.GetKey()), "/ipns/") {
				// Extract the PID...
				pidbytes := req.GetKey()[6:]
				pid, err := peer.IDFromBytes(pidbytes)
				if err == nil {
					s := fmt.Sprintf("%s,%s", peerID, pid.Pretty())
					dht.log_get_ipns.WriteData(s)
				}
			}
		case 2:
			atomic.AddUint64(&dht.metric_read_add_provider, 1)

			pinfos := pb.PBPeersToPeerInfos(req.GetProviderPeers())

			_, cid, err := cid.CidFromBytes(req.GetKey())
			if err == nil {

				// Log all providers...
				for _, pi := range pinfos {
					for _, a := range pi.Addrs {
						s := fmt.Sprintf("%s,%s,%s,%s", peerID, cid, pi.ID, a)
						dht.log_addproviders.WriteData(s)
					}
				}
			}
		case 3:
			atomic.AddUint64(&dht.metric_read_get_provider, 1)

			_, cid, err := cid.CidFromBytes(req.GetKey())
			if err == nil {
				s := fmt.Sprintf("%s,%s", peerID, cid)
				dht.log_getproviders.WriteData(s)
			}
		case 4:
			atomic.AddUint64(&dht.metric_read_find_node, 1)
		case 5:
			atomic.AddUint64(&dht.metric_read_ping, 1)
			// We need to send back a PONG...
		}

		// Check out the FIND_NODE on that!
		for _, cpeer := range req.CloserPeers {

			pid, err := peer.IDFromBytes([]byte(cpeer.Id))
			if err == nil {
				for _, a := range cpeer.Addrs {
					ad, err := multiaddr.NewMultiaddrBytes(a)
					if err == nil && isConnectable(ad) {

						dht.hostmu.Lock()
						dht.peerstore.AddAddr(pid, ad, 1*time.Hour)
						dht.hostmu.Unlock()

						// localPeerID, fromPeerID, newPeerID, addr
						s := fmt.Sprintf("%s,%s,%s,%s", localPeerID, peerID, pid, ad)
						dht.log_peerinfo.WriteData(s)
					}
				}

				atomic.AddUint64(&dht.metric_peers_found, 1)

				// Add it to our node details
				dht.nodedetails.Add(pid.Pretty())
			}
		}
	}
}

// Filter out some common unconnectable addresses...
func isConnectable(a multiaddr.Multiaddr) bool {

	// Loopbacks
	if strings.HasPrefix(a.String(), "/ip4/127.0.0.1/") ||
		strings.HasPrefix(a.String(), "/ip6/::1/") {
		return false
	}

	// Internal ip4 ranges
	if strings.HasPrefix(a.String(), "/ip4/192.168.") ||
		strings.HasPrefix(a.String(), "/ip4/10.") ||
		strings.HasPrefix(a.String(), "/ip4/172.16.") ||
		strings.HasPrefix(a.String(), "/ip4/172.17.") ||
		strings.HasPrefix(a.String(), "/ip4/172.18.") ||
		strings.HasPrefix(a.String(), "/ip4/172.19.") ||
		strings.HasPrefix(a.String(), "/ip4/172.20.") ||
		strings.HasPrefix(a.String(), "/ip4/172.21.") ||
		strings.HasPrefix(a.String(), "/ip4/172.22.") ||
		strings.HasPrefix(a.String(), "/ip4/172.23.") ||
		strings.HasPrefix(a.String(), "/ip4/172.24.") ||
		strings.HasPrefix(a.String(), "/ip4/172.25.") ||
		strings.HasPrefix(a.String(), "/ip4/172.26.") ||
		strings.HasPrefix(a.String(), "/ip4/172.27.") ||
		strings.HasPrefix(a.String(), "/ip4/172.28.") ||
		strings.HasPrefix(a.String(), "/ip4/172.29.") ||
		strings.HasPrefix(a.String(), "/ip4/172.30.") ||
		strings.HasPrefix(a.String(), "/ip4/172.31.") {
		return false
	}
	return true
}

// Process a peer stream
func (dht *DHT) ProcessPeerStream(ctx context.Context, cancelFunc context.CancelFunc, s network.Stream) {
	pid := s.Conn().RemotePeer()

	dht.WritePeerInfo(pid)

	// Start something up to periodically FIND_NODE and PING
	go dht.doPeriodicWrites(ctx, cancelFunc, s)

	// Start something to read messages
	go dht.doReading(ctx, cancelFunc, s)
}

// WritePeerInfo - write some data from our peerstore for pid
func (dht *DHT) WritePeerInfo(pid peer.ID) {
	dht.hostmu.Lock()
	// Find out some info about the peer...
	protocols, protoerr := dht.peerstore.GetProtocols(pid)
	agent, agenterr := dht.peerstore.Get(pid, "AgentVersion")
	dht.hostmu.Unlock()

	if protoerr == nil {
		for _, proto := range protocols {
			s := fmt.Sprintf("%s,%s", pid.Pretty(), proto)
			dht.log_peer_protocols.WriteData(s)
		}
	}

	if agenterr == nil {
		s := fmt.Sprintf("%s,%s", pid.Pretty(), agent)
		dht.log_peer_agents.WriteData(s)
	}

	decoded, err := mh.Decode([]byte(pid))
	if err == nil {
		s := fmt.Sprintf("%s,%s,%d,%x", pid.Pretty(), decoded.Name, decoded.Length, decoded.Digest)
		dht.log_peer_ids.WriteData(s)
	}

}
