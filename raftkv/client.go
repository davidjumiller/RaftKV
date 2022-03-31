package raftkv

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

type Client struct {
	clientId string
}

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
	LowerOpId         uint32
	UpperOpId         uint32
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

func MakeClient(servers []*ClientEnd) *Client {
	return nil
}

func (c *Client) Start(key string) (NotifyChannel, error) {
	return nil, nil
}
func (c *Client) Get(key string) error {
	return nil
}
func (c *Client) Put(key string, value string) error {
	return nil
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, localServerIPPort string, serverIPPortList []string, chCapacity int) (NotifyChannel, error) {
	d.NotifyCh = make(NotifyChannel, chCapacity)
	d.KTrace = localTracer.CreateTrace()
	d.ClientId = clientId
	d.LocalServerAddr = localServerIPPort
	d.Tracer = localTracer
	d.ServerList = serverIPPortList
	remote := NewRemoteKVS()
	remote.KVS = d

	// Tracing
	d.KTrace.RecordAction(KvslibStart{clientId})

	// Setup RPC
	err := rpc.RegisterName("KVS", remote)
	if err != nil {
		return nil, err
	}
	d.RemoteAddrIndex = 0 // Connect with the first server on the list
	serverAddr := d.ServerList[d.RemoteAddrIndex]
	d.Conn, d.Client = makeClient(d.LocalServerAddr, serverAddr)
	// M2: handle failed/non-responsive servers

	// Receive calls from the server
	d.ServerListener, err = startRPCListener(serverAddr)
	util.CheckErr(err, "Could not start serverAddr listener in kvslib Start")
	go remote.serveRequests()
	return d.NotifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value OpId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) error {
	d.Mutex.RLock()
	numOutstanding, exists := d.Puts[key]
	d.Mutex.RUnlock()
	var localOpId uint32
	if exists {
		localOpId = d.UpperOpId
		d.UpperOpId++

		if numOutstanding > 0 {
			// Outstanding put(s); buffer for later
			getArgs := d.createGetArgs(tracer, clientId, key, localOpId)
			bufferedGet := BufferedGet{
				Args:    getArgs,
				PutOpId: d.LastPutOpId,
			}
			d.BufferedGets[key].PushBack(bufferedGet)
			return nil
		}
	} else {
		localOpId = d.LowerOpId
		d.LowerOpId++
		if d.LowerOpId == uint32(math.Pow(2, 16)) {
			temp := d.UpperOpId
			d.LowerOpId = temp
			d.UpperOpId = (uint32(math.Pow(2, 32)) - d.LowerOpId) / 2
		}
	}
	getArgs := d.createGetArgs(tracer, clientId, key, localOpId)
	d.sendGet(getArgs)
	return nil

}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) error {
	// Should return OpId or error
	localOpId := d.UpperOpId
	d.LastPutOpId = localOpId
	d.UpperOpId++

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
	trace.RecordAction(Put{clientId, localOpId, key, value})

	// Send put to head via RPC
	putArgs := &PutArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GId:          0,
		Value:        value,
		Token:        trace.GenerateToken(),
		ClientIPPort: d.LocalServerAddr, // Receives result from tail
	}
	d.Mutex.Lock()
	d.InProgress[localOpId] = time.Now()
	d.Mutex.Unlock()
	var gid uint64
	err := d.Client.Call("Server.Put", putArgs, &gid)
	if err != nil {
		return err
	}

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

// Serve client requests for a RemoteKVS while the KVS is alive
func (remoteKVS *RemoteKVS) serveRequests() {
	for {
		select {
		case <-remoteKVS.KVS.AliveCh:
			return
		case <-time.After(time.Second):
			rpc.Accept(remoteKVS.KVS.ServerListener)
		}
	}
}

// PutResult Confirms that a Put succeeded
// Does not reply to callee!
func (remoteKVS *RemoteKVS) PutResult(args *PutRes, _ *interface{}) error {
	remoteKVS.KVS.Mutex.Lock()
	_, exists := remoteKVS.KVS.InProgress[args.OpId]
	if !exists {
		// Do nothing
		return nil
	}
	remoteKVS.updateInProgressAndRtt(args.OpId)
	remoteKVS.KVS.Mutex.Unlock()

	trace := remoteKVS.KVS.Tracer.ReceiveToken(args.PToken)
	trace.RecordAction(PutResultRecvd{
		OpId: args.OpId,
		GId:  args.GId,
		Key:  args.Key,
	})
	result := ResultStruct{
		OpId:   args.OpId,
		GId:    args.GId,
		Result: args.Value,
	}

	// Update data structures
	remoteKVS.KVS.Mutex.Lock()
	num, _ := remoteKVS.KVS.Puts[args.Key]
	remoteKVS.KVS.Puts[args.Key] = num - 1
	remoteKVS.KVS.LastPutResultOpId = args.OpId
	remoteKVS.KVS.Mutex.Unlock()
	remoteKVS.KVS.sendBufferedGets(args.Key)

	// Send result
	remoteKVS.KVS.NotifyCh <- result
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
	remoteKVS.KVS.Mutex.RLock()
	_, exists := remoteKVS.KVS.InProgress[args.OpId]
	remoteKVS.KVS.Mutex.RUnlock()
	if !exists {
		// Do nothing
		return nil
	}
	remoteKVS.updateInProgressAndRtt(args.OpId)

	trace := remoteKVS.KVS.Tracer.ReceiveToken(args.GToken)
	trace.RecordAction(GetResultRecvd{
		OpId:  args.OpId,
		GId:   args.GId,
		Key:   args.Key,
		Value: args.Value,
	})
	result := ResultStruct{
		OpId:   args.OpId,
		GId:    args.GId,
		Result: args.Value,
	}
	remoteKVS.KVS.NotifyCh <- result
	return nil
}

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

// Creates GetArgs struct for a new Get
func (d *KVS) createGetArgs(tracer *tracing.Tracer, clientId string, key string, localOpId uint32) *GetArgs {
	trace := tracer.CreateTrace()
	serverAddr := d.ServerList[d.RemoteAddrIndex]
	getArgs := &GetArgs{
		ClientId:     clientId,
		OpId:         localOpId,
		Key:          key,
		GToken:       trace.GenerateToken(),
		ClientIPPort: serverAddr, // Receives result from tail
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
	d.Client.Go("Server.Get", getArgs, nil, nil)

	// Result should be received from tail via KVS.GetResult()
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
func (remoteKVS *RemoteKVS) updateInProgressAndRtt(opId uint32) {
	remoteKVS.KVS.Mutex.Lock()
	newRtt := time.Now().Sub(remoteKVS.KVS.InProgress[opId])
	remoteKVS.KVS.RTT = (remoteKVS.KVS.RTT + newRtt) / 2
	delete(remoteKVS.KVS.InProgress, opId)
	remoteKVS.KVS.Mutex.Unlock()
}
