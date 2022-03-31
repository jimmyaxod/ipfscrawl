package dht

import (
	"sync/atomic"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	p_read_ping = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_ping", Help: ""})
	p_read_find_node = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_find_node", Help: ""})
	p_read_add_provider = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_add_provider", Help: ""})
	p_read_get_providers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_get_providers", Help: ""})
	p_read_put_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_put_value", Help: ""})
	p_read_get_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_read_get_value", Help: ""})

	p_written_ping = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_ping", Help: ""})
	p_written_find_node = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_find_node", Help: ""})
	p_written_add_provider = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_add_provider", Help: ""})
	p_written_get_providers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_get_providers", Help: ""})
	p_written_put_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_put_value", Help: ""})
	p_written_get_value = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dht_written_get_value", Help: ""})
)

type DHTSessionMgr struct {
	metric_written_ping          uint64
	metric_written_find_node     uint64
	metric_written_add_provider  uint64
	metric_written_get_providers uint64
	metric_written_put_value     uint64
	metric_written_get_value     uint64

	metric_read_ping          uint64
	metric_read_find_node     uint64
	metric_read_add_provider  uint64
	metric_read_get_providers uint64
	metric_read_put_value     uint64
	metric_read_get_value     uint64
}

func NewDHTSessionMgr() *DHTSessionMgr {
	return &DHTSessionMgr{}
}

func (mgr *DHTSessionMgr) RegisterRead(msg pb.Message) {
	switch msg.Type {
	case pb.Message_PING:
		p_read_ping.Inc()
		atomic.AddUint64(&mgr.metric_read_ping, 1)
	case pb.Message_FIND_NODE:
		p_read_find_node.Inc()
		atomic.AddUint64(&mgr.metric_read_find_node, 1)
	case pb.Message_ADD_PROVIDER:
		p_read_add_provider.Inc()
		atomic.AddUint64(&mgr.metric_read_add_provider, 1)
	case pb.Message_GET_PROVIDERS:
		p_read_get_providers.Inc()
		atomic.AddUint64(&mgr.metric_read_get_providers, 1)
	case pb.Message_PUT_VALUE:
		p_read_put_value.Inc()
		atomic.AddUint64(&mgr.metric_read_put_value, 1)
	case pb.Message_GET_VALUE:
		p_read_get_value.Inc()
		atomic.AddUint64(&mgr.metric_read_get_value, 1)
	}
}

func (mgr *DHTSessionMgr) RegisterWritten(msg pb.Message) {
	switch msg.Type {
	case pb.Message_PING:
		p_written_ping.Inc()
		atomic.AddUint64(&mgr.metric_written_ping, 1)
	case pb.Message_FIND_NODE:
		p_written_find_node.Inc()
		atomic.AddUint64(&mgr.metric_written_find_node, 1)
	case pb.Message_ADD_PROVIDER:
		p_written_add_provider.Inc()
		atomic.AddUint64(&mgr.metric_written_add_provider, 1)
	case pb.Message_GET_PROVIDERS:
		p_written_get_providers.Inc()
		atomic.AddUint64(&mgr.metric_written_get_providers, 1)
	case pb.Message_PUT_VALUE:
		p_written_put_value.Inc()
		atomic.AddUint64(&mgr.metric_written_put_value, 1)
	case pb.Message_GET_VALUE:
		p_written_get_value.Inc()
		atomic.AddUint64(&mgr.metric_written_get_value, 1)
	}
}
