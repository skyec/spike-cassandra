package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/gocql/gocql"
)

func main() {
	var (
		servers  string
		keys     string
		sliceLen int
	)
	flag.StringVar(&servers, "s", os.Getenv("CASSANDRA"), "comma separated list of Cassandra server to connect to")
	flag.StringVar(&keys, "keys", "dddddd8765432111111117", "comma separated list of keys to query")
	flag.IntVar(&sliceLen, "slice-len", 100, "The number of columns to fetch")
	flag.Parse()

	cList := strings.Split(servers, ",")
	log.Println("Servers:", strings.Join(cList, ","))

	cluster := gocql.NewCluster(cList...)
	cluster.Keyspace = "spike"
	cluster.Consistency = gocql.One
	cluster.ConnPoolType = gocql.NewRoundRobinConnPool
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Error creating session: %s", err)
	}
	defer session.Close()

	for {
		var rid, event string
		iter := session.Query(`SELECT resource_id, event FROM resources_simple LIMIT ?`, sliceLen).Iter()
		for iter.Scan(&rid, &event) {
			log.Println("event:", rid, event)
		}
		if err := iter.Close(); err != nil {
			log.Fatalf("Error closing the iterator:", err)
		}
		log.Println("Loop")
	}
}
