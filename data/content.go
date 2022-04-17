package data

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	session *gocql.Session
)

// Prometheus metrics
var (
	p_contentdb_writes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contentdb_writes", Help: ""})
	p_contentdb_writes_dupe = promauto.NewCounter(prometheus.CounterOpts{
		Name: "contentdb_writes_dupe", Help: ""})
)

func CQLConnect() {
	var err error
	for {
		cluster := gocql.NewCluster("127.0.0.1")
		cluster.Keyspace = "ipfscrawl"
		cluster.Consistency = gocql.Quorum
		session, err = cluster.CreateSession()
		if err == nil {
			return
		}
		fmt.Printf("CQL: Connecting %v\n", err)
		time.Sleep(10 * time.Second)
	}
}

func InsertContent(id string, links string, datasize int) {
	if session == nil {
		CQLConnect()
	}

	q := session.Query(`INSERT INTO content (id, links, datasize) VALUES (?, ?, ?) IF NOT EXISTS`, id, links, datasize)

	// Try to do the insert and see if it gets applied...
	dest := make(map[string]interface{})
	applied, err := q.MapScanCAS(dest)

	if err == nil {
		if applied {
			p_contentdb_writes.Inc()
		} else {
			p_contentdb_writes_dupe.Inc()
		}
	} else {
		fmt.Printf("InsertContent err %v\n", err)
		session.Close()
		CQLConnect()
		fmt.Printf("Reconnected\n")
	}
}
