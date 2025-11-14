package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"

	"github.com/Bromles/epaxos-fixed/src/bindings"
	"github.com/Bromles/epaxos-fixed/src/state"
	"github.com/google/uuid"
)

var (
	clientId    = *flag.String("id", "", "the id of the client. Default is RFC 4122 nodeID.")
	masterAddr  = flag.String("maddr", "", "Master address. Defaults to localhost")
	masterPort  = flag.Int("mport", 7087, "Master port. ")
	reqsNb      = flag.Int("q", 1000, "Total number of requests. ")
	writes      = flag.Int("w", 100, "Percentage of updates (writes). ")
	payloadSize = flag.Int("payloadSize", 100, "Payload size for writes.")
	fast        = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. ")
	localReads  = flag.Bool("l", false, "Execute reads at the closest (local) replica. ")
	procs       = flag.Int("p", 2, "GOMAXPROCS. ")
	conflicts   = flag.Int("c", 0, "Percentage of conflicts. Defaults to 0%")
	verbose     = flag.Bool("v", false, "verbose mode. ")
	scan        = flag.Bool("s", false, "replace read with short scan (100 elements)")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	var proxy *bindings.Parameters
	for {
		proxy = bindings.NewParameters(*masterAddr, *masterPort, *verbose, *fast, *localReads)
		err := proxy.Connect()
		if err == nil {
			break
		}
		proxy.Disconnect()
	}

	if clientId == "" {
		clientId = uuid.New().String()
	}

	karray := make([]state.Key, *reqsNb)
	put := make([]bool, *reqsNb)

	clientKey := state.Key(uint64(uuid.New().Time())) // a command id unique to this client.
	log.Printf("client: %v (verbose=%v, payloadSize=%v, conflicts=%v, key=%v)", clientId, *verbose, *payloadSize, *conflicts, clientKey)
	for i := 0; i < *reqsNb; i++ {
		put[i] = false
		if *writes > 0 {
			r := rand.Intn(100)
			if r <= *writes {
				put[i] = true
			}
		}
		karray[i] = clientKey
		if *conflicts > 0 {
			r := rand.Intn(100)
			if r <= *conflicts {
				karray[i] = 42
			}
		}
	}

	beforeTotal := time.Now()

	for j := 0; j < *reqsNb; j++ {

		before := time.Now()

		key := int64(karray[j])

		if put[j] {
			value := make([]byte, *payloadSize)
			// todo figure out if it is safe to replace with non-deterministic method
			rand.Read(value)
			proxy.Write(key, value)
		} else {
			if *scan {
				proxy.Scan(key, int64(100))
			} else {
				proxy.Read(key)
			}
		}

		after := time.Now()

		duration := after.Sub(before)
		fmt.Printf("latency %d\n", toMs(duration.Nanoseconds()))
		fmt.Printf("chain %d-1\n", toMs(after.UnixNano()))
	}

	fmt.Printf(proxy.Stats() + "\n")

	proxy.Disconnect()

	afterTotal := time.Now()
	fmt.Printf("Test took %v\n", afterTotal.Sub(beforeTotal))
}

// convert nanosecond to millisecond
func toMs(nano int64) int64 {
	return nano / int64(time.Millisecond)
}
