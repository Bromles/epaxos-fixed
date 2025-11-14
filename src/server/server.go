package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/Bromles/epaxos-fixed/src/epaxos"
	"github.com/Bromles/epaxos-fixed/src/masterproto"
)

var (
	portnum             = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
	masterAddr          = flag.String("maddr", "", "Master address. Defaults to localhost.")
	masterPort          = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
	myAddr              = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
	procs               = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
	cpuprofile          = flag.String("cpuprofile", "", "write cpu profile to file")
	exec                = flag.Bool("exec", true, "Execute commands.")
	lread               = flag.Bool("lread", false, "Execute locally read command.")
	dreply              = flag.Bool("dreply", true, "Reply to client only after command has been executed.")
	beacon              = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
	maxfailures         = flag.Int("maxfailures", -1, "maximum number of maxfailures; default is a minority, ignored by other protocols than Paxos.")
	durable             = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
	batchWait           = flag.Int("batchwait", 0, "Milliseconds to wait before sending a batch. If set to 0, batching is disabled. Defaults to 0.")
	transitiveConflicts = flag.Bool("transitiveconf", true, "Conflict relation is transitive.")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))

	*maxfailures = (len(nodeList) - 1) / 2

	log.Printf("Tolerating %d max. failures\n", *maxfailures)

	log.Println("Starting Egalitarian Paxos replica...")
	rep := epaxos.NewReplica(replicaId, nodeList, *exec, *lread, *dreply, *beacon, *durable, *batchWait, *transitiveConflicts, *maxfailures)
	rpc.Register(rep)

	rpc.HandleHTTP()
	// listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func registerWithMaster(masterAddr string) (int, []string) {
	args := &masterproto.RegisterArgs{Addr: *myAddr, Port: *portnum}
	var reply masterproto.RegisterReply

	for done := false; !done; {
		log.Printf("connecting to: %v", masterAddr)
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
		}
		if err != nil {
			log.Printf("%v", err)
		}
		time.Sleep(1e9)
	}

	return reply.ReplicaId, reply.NodeList
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
