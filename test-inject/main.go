// Randmo data injector
package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

var resourceIds []string

func main() {

	log.SetFlags(log.Lmicroseconds)
	log.SetPrefix("CS ")

	var (
		servers string
		count   int
		clients int
		keys    int
	)

	flag.StringVar(&servers, "s", os.Getenv("CASSANDRA"), "comma separated list of Cassandra server to connect to")
	flag.IntVar(&count, "count", 10000, "Number of writes to inject")
	flag.IntVar(&clients, "clients", 1, "Number of concurrent clients to use")
	flag.IntVar(&keys, "keys", 20, "Number of unique keys randomly written to for the test")
	flag.Parse()

	generation := time.Now().Unix()
	rand.Seed(generation)

	if clients < 1 {
		clients = 1
	}

	if keys < 10 {
		keys = 10
	}

	cList := strings.Split(servers, ",")
	log.Println("Servers:", strings.Join(cList, ","))
	log.Println("Count:", count)
	log.Println("Clients", clients)
	log.Println("Keys", keys)

	log.Println("Connect to the cluster ...")
	cluster := gocql.NewCluster(cList...)
	cluster.Keyspace = "spike"
	cluster.Consistency = gocql.One
	cluster.ConnPoolType = gocql.NewRoundRobinConnPool
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Error creating session: %s", err)
	}
	defer session.Close()

	log.Println("Conencted!")

	var stats struct {
		Count      int
		Errors     int
		LatencySum int
		MaxLatency int
		MinLatency int
	}
	statsTic := func(n int) {
		stats.Count++
		stats.LatencySum += n
		if n > stats.MaxLatency {
			stats.MaxLatency = n
		}
		if n < stats.MinLatency {
			stats.MinLatency = n
		}
	}

	chCount := make(chan int, count)
	chError := make(chan struct{}, count)

	wgStats := &sync.WaitGroup{}
	wgClients := &sync.WaitGroup{}

	// Stats collection goroutine
	go func() {
		wgStats.Add(1)
		printCount := 0
		for {

			select {
			case n, ok := <-chCount:
				if !ok {
					wgStats.Done()
					return
				}
				statsTic(n)
			case <-chError:
				stats.Errors++
				// don't compute latency stats on errors!
			}
			printCount++
			if printCount%500 == 0 {
				log.Println("Count:", printCount)
			}
		}
	}()

	log.Println("Generating work queue ...")
	chWork := make(chan int, count)
	for i := 0; i < count; i++ {
		chWork <- i
	}

	log.Println("Generating keys ...")
	generateKeys(keys)

	stats.MinLatency = math.MaxInt64
	log.Println("Starting injection ....")
	fullStart := time.Now()

	for i := 0; i < clients; i++ {
		wgClients.Add(1)
		go func() {
			resCount := len(resourceIds)
			for range chWork {
				rid := resourceIds[rand.Intn(resCount)]
				rid += "-" + strconv.FormatInt(generation, 10)
				start := time.Now()

				err := injectEvent(session, rid)

				latency := time.Now().Sub(start)

				if err != nil {
					chError <- struct{}{}
				} else {
					chCount <- int(latency)
				}
			}
			wgClients.Done()
		}()
	}
	close(chWork)
	wgClients.Wait()

	fullDuration := time.Now().Sub(fullStart)
	close(chCount)
	wgStats.Wait()

	seconds := float32(fullDuration) / float32(time.Second)

	log.Println("Done!")
	log.Println("Count:", stats.Count)
	log.Println("Errors:", stats.Errors)
	log.Printf("Time: %.4fs", float32(fullDuration)/float32(time.Second))
	log.Printf("Writes/Second: %9.4f", float32(stats.Count)/seconds)
	log.Println("Latency:")
	log.Printf("   Avg: %9.4fms", float32(stats.LatencySum)/float32(stats.Count)/float32(time.Millisecond))
	log.Printf("   Min: %9.4fms", float32(stats.MinLatency)/float32(time.Millisecond))
	log.Printf("   Max: %9.4fms", float32(stats.MaxLatency)/float32(time.Millisecond))
}

func injectEvent(session *gocql.Session, resId string) error {
	t := time.Now().UTC()
	err := session.Query(`INSERT INTO resources_simple(event_time, resource_id, event) VALUES (?, ?, ?)`,
		t, resId, fmt.Sprintf(`{"message":"Hello World", "ts":"%s"}`, t.Format(time.RFC3339Nano))).Exec()

	if err != nil {
		log.Printf("Error with insert for id: %s: %s", resId, err)
		return fmt.Errorf("insert error:", err)
	}
	return nil
}

func generateKeys(n int) {
	resourceIds = make([]string, n)
	h := md5.New()
	io.WriteString(h, time.Now().Format(time.RFC3339Nano))

	for i := 0; i < n; i++ {
		io.WriteString(h, "-")
		io.WriteString(h, strconv.Itoa(n))
		newKey := fmt.Sprintf("%x", h.Sum(nil))
		resourceIds = append(resourceIds, newKey)
		log.Println("KEY:", newKey)
	}
}
