package kvslib

import (
	"container/list"
	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
	"math"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type PutArgs struct {
	ClientId     string
	OpId         uint32
	GId          uint64
	Key          string
	Value        string
	Token        tracing.TracingToken
	ClientIPPort string
}

type PutRes struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string
	PToken tracing.TracingToken
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

type GetArgs struct {
	ClientId     string
	OpId         uint32
	Key          string
	GToken       tracing.TracingToken
	ClientIPPort string
}

type GetRes struct {
	OpId   uint32
	GId    uint64
	Key    string
	Value  string // Note: this should be "" if a Put for this key does not exist
	GToken tracing.TracingToken
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

type KVS struct {
	NotifyCh NotifyChannel
	// Add more KVS instance state here.
	KTrace            *tracing.Trace
	ClientId          string
	LocalServerAddr   string
	RemoteCoordAddr   string
	LocalHeadAddr     string
	RemoteHeadAddr    string
	LocalTailAddr     string
	RemoteTailAddr    string
	HeadListener      *net.TCPListener
	TailListener      *net.TCPListener
	RTT               time.Duration
	Tracer            *tracing.Tracer
	InProgress        map[uint32]time.Time // Map representing sent requests that haven't been responded to
	Mutex             *sync.RWMutex
	Puts              map[string]uint32 // int representing number of puts on the key
	LastPutOpId       uint32            // int representing last op id assigned to a put
	LastPutResultOpId uint32            // int representing the op id of the last put result received
	BufferedGets      map[string]*list.List
	LowerOpId         uint32
	UpperOpId         uint32
	AliveCh           chan int
}

func NewKVS() *KVS {
	return &KVS{
		NotifyCh:          nil,
		RemoteHeadAddr:    "",
		RemoteTailAddr:    "",
		HeadListener:      nil,
		TailListener:      nil,
		InProgress:        make(map[uint32]time.Time),
		Mutex:             new(sync.RWMutex),
		Puts:              make(map[string]uint32),
		LastPutOpId:       0,
		LastPutResultOpId: 0,
		BufferedGets:      make(map[string]*list.List),
		LowerOpId:         0,
		UpperOpId:         uint32(math.Pow(2, 16)),
		RTT:               3 * time.Second,
		AliveCh:           make(chan int),
	}
}

type RemoteKVS struct {
	KVS *KVS
}

func NewRemoteKVS() *RemoteKVS {
	return &RemoteKVS{
		KVS: nil,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, localServerIPPort string, serverIPPortList []string, chCapacity int) (NotifyChannel, error) {
	return d.NotifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value OpId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) error {
	// Should return OpId or error
	return nil

}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) error {
	return nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	return
}

// PutResult Confirms that a Put succeeded
// Does not reply to callee!
func (remoteKVS *RemoteKVS) PutResult(args *PutRes, _ *interface{}) error {
	return nil
}

// Creates and returns a TCP connection between localAddr and remoteAddr
func makeConnection(localAddr string, remoteAddr string) *net.TCPConn {
	localTcpAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	util.CheckErr(err, "Could not resolve address: "+localAddr)
	remoteTcpAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	util.CheckErr(err, "Could not resolve address: "+remoteAddr)
	conn, err := net.DialTCP("tcp", localTcpAddr, remoteTcpAddr)
	util.CheckErr(err, "Could not connect "+localAddr+" to "+remoteAddr)

	return conn
}

// GetResult Confirms that a Get succeeded
// Does not reply to callee!
func (remoteKVS *RemoteKVS) GetResult(args *GetRes, _ *interface{}) error {
	return nil
}

// Code from ubcars server implementation (chainedkv/server.go)
// Starts an RPC listener and returns the address it is on
func startRPCListener(rpcListenAddr string) (*net.TCPListener, error) {
	resolvedRPCListenAddr, err := net.ResolveTCPAddr("tcp", rpcListenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", resolvedRPCListenAddr)
	if err != nil {
		return nil, err
	}
	go rpc.Accept(listener)
	return listener, nil
}

// Creates an RPC client given between a local and remote address
func makeClient(localAddr string, remoteAddr string) (*net.TCPConn, *rpc.Client) {
	conn := makeConnection(localAddr, remoteAddr)
	client := rpc.NewClient(conn)
	return conn, client
}
