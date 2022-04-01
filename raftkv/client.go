package raftkv

import (
	"container/list"
	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// ClientEnd represents a client endpoint
type ClientEnd struct {
	Addr   string
	Client *rpc.Client
}

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

type PutArgs struct {
	ClientId     string
	OpId         uint32
	Key          string
	Value        string
	Token        tracing.TracingToken
	ClientIPPort string
}

type PutRes struct {
	OpId   uint32
	Key    string
	Value  string
	PToken tracing.TracingToken
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type BufferedGet struct {
	Args    *GetArgs
	PutOpId uint32
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
	Key    string
	Value  string // Note: this should be "" if a Put for this key does not exist
	GToken tracing.TracingToken
}

// NotifyChannel is used for notifying the client about a result for an operation.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	Result string
}

type KVS struct {
	NotifyCh NotifyChannel
	// Add more KVS instance state here.
	KTrace            *tracing.Trace
	ClientId          string
	LocalServerAddr   string
	RemoteAddrIndex   int
	ServerList        []string
	ServerListener    *net.TCPListener
	RTT               time.Duration
	Tracer            *tracing.Tracer
	InProgress        map[uint32]time.Time // Map representing sent requests that haven't been responded to
	Mutex             *sync.RWMutex
	Puts              map[string]uint32 // int representing number of puts on the key
	LastPutOpId       uint32            // int representing last op id assigned to a put
	LastPutResultOpId uint32            // int representing the op id of the last put result received
	BufferedGets      map[string]*list.List
	OpId              uint32
	AliveCh           chan int
	Conn              *net.TCPConn
	Client            *rpc.Client
}

func NewKVS() *KVS {
	return &KVS{
		NotifyCh:          nil,
		InProgress:        make(map[uint32]time.Time),
		Mutex:             new(sync.RWMutex),
		Puts:              make(map[string]uint32),
		LastPutOpId:       0,
		LastPutResultOpId: 0,
		BufferedGets:      make(map[string]*list.List),
		OpId:              0,
		RTT:               3 * time.Second,
		AliveCh:           make(chan int),
	}
}

type Client struct {
	KVS *KVS
}

// Start Starts the instance of Client to use for connecting to the system.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the system, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, localServerIPPort string, serverIPPortList []string, chCapacity int) (NotifyChannel, error) {
	d.NotifyCh = make(NotifyChannel, chCapacity)
	d.KTrace = localTracer.CreateTrace()
	d.ClientId = clientId
	d.LocalServerAddr = localServerIPPort
	d.Tracer = localTracer
	d.ServerList = serverIPPortList

	// Tracing
	d.KTrace.RecordAction(KvslibStart{clientId})

	d.RemoteAddrIndex = 0 // Connect with the first server on the list
	serverAddr := d.ServerList[d.RemoteAddrIndex]
	d.Conn, d.Client = makeClient(d.LocalServerAddr, serverAddr)
	// M2: handle failed/non-responsive servers
	return d.NotifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value OpId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, key string) error {
	d.Mutex.RLock()
	numOutstanding, exists := d.Puts[key]
	d.Mutex.RUnlock()
	localOpId := d.OpId
	d.OpId++
	if exists {
		if numOutstanding > 0 {
			// Outstanding put(s); buffer for later
			getArgs := d.createGetArgs(tracer, key, localOpId)
			bufferedGet := BufferedGet{
				Args:    getArgs,
				PutOpId: d.LastPutOpId,
			}
			d.BufferedGets[key].PushBack(bufferedGet)
			return nil
		}
	}
	getArgs := d.createGetArgs(tracer, key, localOpId)
	d.sendGet(getArgs) // TODO change to goroutine?
	return nil         // TODO change return value?

}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, key string, value string) error {
	// Should return OpId or error
	localOpId := d.OpId
	d.LastPutOpId = localOpId
	d.OpId++

	// Update map to have an outstanding put
	d.Mutex.Lock()
	num, exists := d.Puts[key]
	if exists {
		d.Puts[key] = num + 1
	} else {
		d.Puts[key] = 1
		d.BufferedGets[key] = list.New()
	}
	d.Mutex.Unlock()

	trace := tracer.CreateTrace()
	trace.RecordAction(Put{d.ClientId, localOpId, key, value})

	// Send put to head via RPC
	putArgs := &PutArgs{
		ClientId:     d.ClientId,
		OpId:         localOpId,
		Key:          key,
		Value:        value,
		Token:        trace.GenerateToken(),
		ClientIPPort: d.LocalServerAddr, // Receives result from tail
	}
	d.Mutex.Lock()
	d.InProgress[localOpId] = time.Now()
	d.Mutex.Unlock()
	var putResult *PutRes
	d.Client.Go("Server.Put", putArgs, putResult, nil)

	// Result should be received from tail via KVS.PutResult()
	//go d.handlePutTimeout(putArgs) // M2: handle Put timeout
	return nil
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	// pass tracer
	d.KTrace.RecordAction(KvslibStop{d.ClientId})
	err := d.Tracer.Close()
	util.CheckErr(err, "Could not close KVS tracer")
	err = d.Client.Close()
	util.CheckErr(err, "Could not close KVS RPC client")
	err = d.Conn.Close()
	util.CheckErr(err, "Could not close KVS server connection")
	err = d.ServerListener.Close()
	util.CheckErr(err, "Could not close KVS RPC listener")
	close(d.NotifyCh)
	d.AliveCh <- 1
	close(d.AliveCh)
	return
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

// Creates an RPC client given between a local and remote address
func makeClient(localAddr string, remoteAddr string) (*net.TCPConn, *rpc.Client) {
	conn := makeConnection(localAddr, remoteAddr)
	client := rpc.NewClient(conn)
	return conn, client
}

// Creates GetArgs struct for a new Get
func (d *KVS) createGetArgs(tracer *tracing.Tracer, key string, localOpId uint32) *GetArgs {
	trace := tracer.CreateTrace()
	getArgs := &GetArgs{
		ClientId: d.ClientId,
		OpId:     localOpId,
		Key:      key,
		GToken:   trace.GenerateToken(),
	}
	return getArgs
}

// Sends a Get request to a server and prepares to receive the result
func (d *KVS) sendGet(getArgs *GetArgs) {
	// Send get to tail via RPC
	trace := d.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(Get{getArgs.ClientId, getArgs.OpId, getArgs.Key})
	d.Mutex.Lock()
	d.InProgress[getArgs.OpId] = time.Now()
	d.Mutex.Unlock()
	var getResult GetRes
	goCall := d.Client.Go("Server.Get", getArgs, &getResult, nil)
	<-goCall.Done
	resultStruct := ResultStruct{
		OpId:   getArgs.OpId,
		Result: getResult.Value,
	}
	d.NotifyCh <- resultStruct
	//go handleGetTimeout(d, getArgs, conn, client) // M2: handle Get timout
}

// Sends the buffered Gets in a KVS associated with key
func (d *KVS) sendBufferedGets(key string) {
	d.Mutex.Lock()
	bufferedGets := d.BufferedGets[key]
	for bufferedGets.Len() > 0 {
		elem := bufferedGets.Front()
		bufferedGet := elem.Value.(BufferedGet)
		if bufferedGet.PutOpId <= d.LastPutResultOpId {
			bufferedGets.Remove(elem)
			d.sendGet(bufferedGet.Args)
		}
	}
	d.Mutex.Unlock()
}

// Updates a KVS's estimated RTT based on an operation's RTT
func (client *Client) updateInProgressAndRtt(opId uint32) {
	client.KVS.Mutex.Lock()
	newRtt := time.Now().Sub(client.KVS.InProgress[opId])
	client.KVS.RTT = (client.KVS.RTT + newRtt) / 2
	delete(client.KVS.InProgress, opId)
	client.KVS.Mutex.Unlock()
}
