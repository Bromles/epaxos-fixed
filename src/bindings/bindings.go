package bindings

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Bromles/epaxos-fixed/src/genericsmrproto"
	"github.com/Bromles/epaxos-fixed/src/masterproto"
	"github.com/Bromles/epaxos-fixed/src/state"
)

const (
	True        = uint8(1)
	Timeout     = 3 * time.Second
	MaxAttempts = 3
)

type Parameters struct {
	masterAddr     string
	masterPort     int
	verbose        bool
	localReads     bool
	closestReplica int
	isFast         bool
	n              int
	replicaLists   []string
	servers        []net.Conn
	readers        []*bufio.Reader
	writers        []*bufio.Writer
	id             int32
	retries        int32
}

func NewParameters(masterAddr string, masterPort int, verbose bool, fast bool, localReads bool) *Parameters {
	return &Parameters{
		masterAddr,
		masterPort,
		verbose,
		localReads,
		0,
		fast,
		0,
		nil,
		nil,
		nil,
		nil,
		0,
		10,
	}
}

func (b *Parameters) Connect() error {
	var err error

	log.Printf("Dialing master...\n")
	var master *rpc.Client
	master = b.MasterDial()

	log.Printf("Getting replica list from master...\n")
	var replyRL *masterproto.GetReplicaListReply

	// loop until the call succeeds
	// (or too many attempts)
	for i, done := 0, false; !done; i++ {
		replyRL = new(masterproto.GetReplicaListReply)
		err = Call(master, "Master.GetReplicaList", new(masterproto.GetReplicaListArgs), replyRL)
		if err == nil && replyRL.Ready {
			done = true
		} else if i == MaxAttempts {
			// if too many attempts, connect again
			err = master.Close()
			if err != nil {
				log.Printf("Failed to close master. Error: %v\n", err)
				return err
			}
			return errors.New("too many call attempts")
		}
	}

	// get closest replica
	err = b.FindClosestReplica(replyRL)
	if err != nil {
		return err
	}
	log.Printf("node list %v, closest (alive) = %v", b.replicaLists, b.closestReplica)

	// init some parameters
	b.n = len(b.replicaLists)
	b.servers = make([]net.Conn, b.n)
	b.readers = make([]*bufio.Reader, b.n)
	b.writers = make([]*bufio.Writer, b.n)

	// get list of nodes to connect to
	var toConnect []int
	toConnect = append(toConnect, b.closestReplica)

	for _, i := range toConnect {
		log.Println("Connection to ", i, " -> ", b.replicaLists[i])
		b.servers[i] = Dial(b.replicaLists[i], false)
		b.readers[i] = bufio.NewReader(b.servers[i])
		b.writers[i] = bufio.NewWriter(b.servers[i])
	}

	log.Println("Connected")

	return nil
}

func Dial(addr string, connect bool) net.Conn {
	var conn net.Conn
	var err error
	var resp *http.Response
	var done bool

	for done = false; !done; {
		conn, err = net.DialTimeout("tcp", addr, Timeout)

		if err == nil {
			if connect {
				// connect if no error
				_, err = io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
				if err != nil {
					log.Printf("Failed to connect to master. Error: %v\n", err)
					return nil
				}
				resp, err = http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
				if err == nil && resp != nil && resp.Status == "200 Connected to Go RPC" {
					done = true
				}
			} else {
				done = true
			}
		}

		if !done {
			// if not done yet, try again
			log.Println("Connection error with ", addr, ": ", err)
			if conn != nil {
				err = conn.Close()
				if err != nil {
					log.Printf("Failed to close master. Error: %v\n", err)
				}
			}
		}
	}

	return conn
}

func (b *Parameters) MasterDial() *rpc.Client {
	var master *rpc.Client
	var addr string
	var conn net.Conn

	addr = fmt.Sprintf("%s:%d", b.masterAddr, b.masterPort)
	conn = Dial(addr, true)
	master = rpc.NewClient(conn)

	return master
}

func Call(cli *rpc.Client, method string, args interface{}, reply interface{}) error {
	c := make(chan error, 1)
	go func() { c <- cli.Call(method, args, reply) }()
	select {
	case err := <-c:
		if err != nil {
			log.Printf("Error in RPC: %s: %s", method, err.Error())
		}
		return err

	case <-time.After(Timeout):
		log.Printf("RPC timeout: " + method)
		return errors.New("RPC timeout")
	}
}

func (b *Parameters) FindClosestReplica(replyRL *masterproto.GetReplicaListReply) error {
	// save replica list and closest
	b.replicaLists = replyRL.ReplicaList

	log.Printf("Pinging all replicas...\n")

	minLatency := math.MaxFloat64
	for i := 0; i < len(b.replicaLists); i++ {
		if !replyRL.AliveList[i] {
			continue
		}
		addr := strings.Split(b.replicaLists[i], ":")[0]
		if addr == "" {
			addr = "127.0.0.1"
		}
		out, err := exec.Command("ping", addr, "-c 3", "-q").Output()
		if err == nil {
			// parse ping output
			latency, _ := strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			log.Printf("%v -> %v", i, latency)

			// save if closest replica
			if minLatency > latency {
				b.closestReplica = i
				minLatency = latency
			}
		} else {
			log.Printf("cannot ping " + b.replicaLists[i])
			return err
		}
	}

	return nil
}

func (b *Parameters) Disconnect() {
	for _, server := range b.servers {
		if server != nil {
			err := server.Close()
			if err != nil {
				log.Printf("Failed to close master. Error: %v\n", err)
				return
			}
		}
	}
	log.Printf("Disconnected")
}

// not idempotent in case of a failure
func (b *Parameters) Write(key int64, value []byte) {
	b.id++
	args := genericsmrproto.Propose{
		CommandId: b.id,
		Command: state.Command{
			Op: state.PUT,
			V:  state.NIL(),
		},
	}
	args.CommandId = b.id
	args.Command.K = state.Key(key)
	args.Command.V = value
	args.Command.Op = state.PUT

	if b.verbose {
		log.Println(args.Command.String())
	}

	b.execute(args)
}

func (b *Parameters) Read(key int64) []byte {
	b.id++
	args := genericsmrproto.Propose{CommandId: b.id, Command: state.Command{Op: state.PUT, V: state.NIL()}}
	args.CommandId = b.id
	args.Command.K = state.Key(key)
	args.Command.Op = state.GET

	if b.verbose {
		log.Println(args.Command.String())
	}

	return b.execute(args)
}

func (b *Parameters) Scan(key int64, count int64) []byte {
	b.id++
	args := genericsmrproto.Propose{CommandId: b.id, Command: state.Command{Op: state.PUT, V: state.NIL()}}
	args.CommandId = b.id
	args.Command.K = state.Key(key)
	args.Command.V = make([]byte, 8)
	binary.LittleEndian.PutUint64(args.Command.V, uint64(count))
	args.Command.Op = state.SCAN

	if b.verbose {
		log.Println(args.Command.String())
	}

	return b.execute(args)
}

func (b *Parameters) Stats() string {
	err := b.writers[b.closestReplica].WriteByte(genericsmrproto.STATS)
	if err != nil {
		log.Printf("Failed to write stats: %v\n", err)
		return ""
	}
	err = b.writers[b.closestReplica].Flush()
	if err != nil {
		log.Printf("Failed to flush stats: %v\n", err)
		return ""
	}
	arr := make([]byte, 1000)
	_, err = b.readers[b.closestReplica].Read(arr)
	if err != nil {
		log.Printf("Failed to read stats: %v\n", err)
		return ""
	}
	return string(bytes.Trim(arr, "\x00"))
}

// internals

func (b *Parameters) execute(args genericsmrproto.Propose) []byte {
	if b.isFast {
		log.Fatal("NYI")
	}

	err := errors.New("")
	value := state.NIL()

	for err != nil {
		submitter := b.closestReplica

		if !b.isFast {
			err = b.writers[submitter].WriteByte(genericsmrproto.PROPOSE)
			if err != nil {
				log.Printf("Failed to write stats: %v\n", err)
				return nil
			}
			args.Marshal(b.writers[submitter])
			err = b.writers[submitter].Flush()
			if err != nil {
				log.Printf("Failed to flush stats: %v\n", err)
				return nil
			}
		} else {
			// send to everyone
			for rep := 0; rep < b.n; rep++ {
				err = b.writers[rep].WriteByte(genericsmrproto.PROPOSE)
				if err != nil {
					log.Printf("Failed to write stats: %v\n", err)
					return nil
				}
				args.Marshal(b.writers[rep])
				err = b.writers[rep].Flush()
				if err != nil {
					log.Printf("Failed to flush stats: %v\n", err)
					return nil
				}
			}
		}

		if b.verbose {
			log.Println("Sent to ", submitter)
		}

		value, err = b.waitReplies(submitter)
		if err != nil {

			log.Println("Error: ", err)

			for err != nil && b.retries > 0 {
				b.retries--
				b.Disconnect()
				log.Println("Reconnecting ...")
				time.Sleep(Timeout) // must be inline with the closest quorum re-computation
				err = b.Connect()
			}

			if err != nil && b.retries == 0 {
				log.Fatal("Cannot recover.")
			}

		}

	}

	if b.verbose {
		log.Println("Returning: ", value.String())
	}

	return value
}

func (b *Parameters) waitReplies(submitter int) (state.Value, error) {
	var err error
	ret := state.NIL()

	rep := new(genericsmrproto.ProposeReplyTS)
	if err = rep.Unmarshal(b.readers[submitter]); err == nil {
		if rep.OK == True {
			ret = rep.Value
		} else {
			err = errors.New("failed to receive a response")
		}
	}

	return ret, err
}
