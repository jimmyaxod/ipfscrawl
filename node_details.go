package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	MaxNumConnectFailures       = 16
	DelayConnectAttemptDuration = 2 * time.Minute
	DelayReconnectDuration      = 1 * time.Hour
)

// NodeInfo contains details about a specific node on the network
type NodeInfo struct {
	id                        string
	foundTime                 time.Time
	lastConnectionAttemptTime time.Time
	lastConnectedTime         time.Time
	numConnectFailures        int
	connected                 bool
}

// NodeDetails contains info about lots of nodes
type NodeDetails struct {
	allIDs []string
	nodes  map[string]*NodeInfo
	mutex  sync.Mutex
}

// NewNodeDetails makes a new NodeDetails
func NewNodeDetails() *NodeDetails {
	nd := &NodeDetails{}
	nd.nodes = make(map[string]*NodeInfo)
	return nd
}

// Stats gets some interesting stats
func (nd *NodeDetails) Stats() string {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	total_nodes := 0
	total_ready := 0
	total_expired := 0
	total_connected := 0

	total_sinceconnect := 0 * time.Second

	for _, info := range nd.nodes {
		total_nodes++
		if readyForConnect(info) {
			total_ready++
		}
		if shouldExpire(info) {
			total_expired++
		}

		if info.connected {
			total_connected++
		}

		total_sinceconnect += time.Since(info.lastConnectedTime)
	}

	avg_since := total_sinceconnect.Seconds() / float64(total_nodes)

	return fmt.Sprintf("NodeDetails nodes=%d connected=%d ready=%d expired=%d avg_since=%.0f seconds\n",
		total_nodes,
		total_connected,
		total_ready,
		total_expired,
		avg_since)
}

// Add adds a node if it doesn't already exist.
func (nd *NodeDetails) Add(id string) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	_, ok := nd.nodes[id]
	if !ok {
		now := time.Now()
		info := &NodeInfo{
			id:                        id,
			foundTime:                 now,
			lastConnectedTime:         now.Add(-24 * time.Hour),
			lastConnectionAttemptTime: now.Add(-24 * time.Hour),
			numConnectFailures:        0,
			connected:                 false,
		}
		nd.allIDs = append(nd.allIDs, id)
		nd.nodes[id] = info
	}
}

// Remove removes a node
func (nd *NodeDetails) Remove(id string) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	_, ok := nd.nodes[id]
	if ok {
		delete(nd.nodes, id)
		for i, val := range nd.allIDs {
			if val == id {
				nd.allIDs[i] = nd.allIDs[len(nd.allIDs)-1]
				nd.allIDs = nd.allIDs[:len(nd.allIDs)-1]
				break
			}
		}
	}
}

// ConnectSuccess Callback for connection success
func (nd *NodeDetails) ConnectSuccess(id string) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	info, ok := nd.nodes[id]
	if ok {
		info.numConnectFailures = 0
		info.lastConnectedTime = time.Now()
	}
}

// ConnectFailure Callback for connection failure
func (nd *NodeDetails) ConnectFailure(id string) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	info, ok := nd.nodes[id]
	if ok {
		info.numConnectFailures++
	}
}

// Connected Callback
func (nd *NodeDetails) Connected(id string) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	info, ok := nd.nodes[id]
	if ok {
		info.connected = true
	}
}

// Disconnected Callback
func (nd *NodeDetails) Disconnected(id string) {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	info, ok := nd.nodes[id]
	if ok {
		info.connected = false
	}
}

// ReadyForConnect checks if a node is ready/due for connection
func (nd *NodeDetails) ReadyForConnect(id string) bool {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()
	info, ok := nd.nodes[id]
	if !ok {
		return false
	}
	if readyForConnect(info) {
		nd.nodes[id].lastConnectionAttemptTime = time.Now()

		return true
	}
	return false
}

// readyForConnect tells us if a node is ready for reconnection
func readyForConnect(ni *NodeInfo) bool {

	if ni.connected {
		return false
	}

	// Next check if there's a recent connection attempt
	if time.Since(ni.lastConnectionAttemptTime) < DelayConnectAttemptDuration {
		return false
	}

	// Next check if it's recently been connected to
	if time.Since(ni.lastConnectedTime) < DelayReconnectDuration {
		return false
	}

	return true
}

func shouldExpire(ni *NodeInfo) bool {
	if ni.numConnectFailures > MaxNumConnectFailures {
		return true
	}
	return false
}

// Get gets a node to attempt to connect to, or "" if it can't atm
func (nd *NodeDetails) Get() string {
	nd.mutex.Lock()
	defer nd.mutex.Unlock()

	// TODO: Could optimize this better...
	n := len(nd.allIDs)
	if n > 0 {
		i := rand.Intn(len(nd.allIDs))
		// Check if it's good...

		id := nd.allIDs[i]
		if readyForConnect(nd.nodes[id]) {
			nd.nodes[id].lastConnectionAttemptTime = time.Now()
			return id
		}

		if shouldExpire(nd.nodes[id]) {
			nd.Remove(id)
		}
	}

	return ""
}
