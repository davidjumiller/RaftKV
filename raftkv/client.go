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

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type Put struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type PutResultRecvd struct {
	ClientId string
	OpId     uint8
	Key      string
}

type Get struct {
	ClientId string
	OpId     uint8
	Key      string
}

type BufferedGet struct {
	Args    *util.GetArgs
	PutOpId uint8
}

type GetResultRecvd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

// NotifyChannel is used for notifying the client about a result for an operation.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint8
	Result string
}

type KVS struct {
	NotifyCh NotifyChannel
	// Add more KVS instance state here.
	KTrace          *tracing.Trace
	ClientId        string
	LocalServerAddr string
	RemoteAddrIndex int
	ServerList      []string
	ServerListener  *net.TCPListener
	RTT             time.Duration
	Tracer          *tracing.Tracer
	InProgress      map[uint8]time.Time // Map representing sent requests that haven't been responded to
	Mutex           *sync.RWMutex
	Puts            map[string]*list.List // list of outstanding put ids for a key
	BufferedGets    map[string]*list.List
	OpId            uint8
	AliveCh         chan int
	Conn            *net.TCPConn
	Client          *rpc.Client
}

func NewKVS() *KVS {
	return &KVS{
		NotifyCh:     nil,
		InProgress:   make(map[uint8]time.Time),
		Mutex:        new(sync.RWMutex),
		Puts:         make(map[string]*list.List),
		BufferedGets: make(map[string]*list.List),
		OpId:         1,
		RTT:          3 * time.Second,
		AliveCh:      make(chan int),
	}
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
	d.Conn, d.Client = util.MakeClient(d.LocalServerAddr, serverAddr)
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
	outstandingPuts, exists := d.Puts[key]
	localOpId := d.OpId
	d.OpId++
	d.Mutex.RUnlock()
	if exists {
		d.Mutex.Lock()
		if outstandingPuts.Len() > 0 {
			// Outstanding put(s); buffer for later
			getArgs := d.createGetArgs(tracer, key, localOpId)
			elem := outstandingPuts.Back() // get latest put opId for this key
			put := elem.Value.(util.PutArgs)
			bufferedGet := BufferedGet{
				Args:    getArgs,
				PutOpId: put.OpId,
			}
			d.BufferedGets[key].PushBack(bufferedGet)
		}
		d.Mutex.Unlock()
	} else {
		getArgs := d.createGetArgs(tracer, key, localOpId)
		go d.sendGet(getArgs)
	}
	return nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, key string, value string) error {
	// Should return OpId or error
	localOpId := d.OpId
	d.OpId++

	trace := tracer.CreateTrace()
	trace.RecordAction(Put{d.ClientId, localOpId, key, value})

	// Send put to head via RPC
	putArgs := &util.PutArgs{
		ClientId: d.ClientId,
		OpId:     localOpId,
		Key:      key,
		Value:    value,
		PToken:   trace.GenerateToken(),
	}
	d.addOutstandingPut(key, putArgs)
	go d.sendPut(localOpId, putArgs)
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

// Creates GetArgs struct for a new Get
func (d *KVS) createGetArgs(tracer *tracing.Tracer, key string, localOpId uint8) *util.GetArgs {
	trace := tracer.CreateTrace()
	getArgs := &util.GetArgs{
		OpId:   localOpId,
		Key:    key,
		GToken: trace.GenerateToken(),
	}
	return getArgs
}

// Sends a Get request to a server and prepares to receive the result
func (d *KVS) sendGet(getArgs *util.GetArgs) {
	// Send get to tail via RPC
	trace := d.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(Get{d.ClientId, getArgs.OpId, getArgs.Key})
	d.Mutex.Lock()
	d.InProgress[getArgs.OpId] = time.Now()
	d.Mutex.Unlock()
	// M2: Refactor receiving into a new function
	var getResult util.GetRes
	goCall := d.Client.Go("KVServer.Get", getArgs, &getResult, nil)
	<-goCall.Done
	resultStruct := ResultStruct{
		OpId:   getArgs.OpId,
		Result: getResult.Value,
	}
	d.NotifyCh <- resultStruct
	trace = d.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(GetResultRecvd{
		ClientId: d.ClientId,
		OpId:     getResult.OpId,
		Key:      getResult.Key,
		Value:    getResult.Value,
	})
	//go handleGetTimeout(d, getArgs, conn, client) // M2: handle Get timout
}

// Sends the buffered Gets to the server matching the given key and opId
func (d *KVS) sendBufferedGets(key string, opId uint8) {
	d.Mutex.Lock()
	bufferedGets := d.BufferedGets[key]
	for bufferedGets.Len() > 0 {
		elem := bufferedGets.Front()
		bufferedGet := elem.Value.(BufferedGet)
		if bufferedGet.PutOpId == opId {
			bufferedGets.Remove(elem)
			d.sendGet(bufferedGet.Args)
		}
	}
	d.Mutex.Unlock()
}

// Sends a put to the server and waits for a result
func (d *KVS) sendPut(localOpId uint8, putArgs *util.PutArgs) {
	d.Mutex.Lock()
	d.InProgress[localOpId] = time.Now()
	d.Mutex.Unlock()
	// M2: Refactor receiving into separate function
	var putResult util.PutRes
	goCall := d.Client.Go("KVServer.Put", putArgs, &putResult, nil)

	<-goCall.Done
	trace := d.Tracer.ReceiveToken(putArgs.PToken)
	trace.RecordAction(PutResultRecvd{
		ClientId: putResult.ClientId,
		OpId:     putResult.OpId,
		Key:      putResult.Key,
	})
	resultStruct := ResultStruct{
		OpId:   putResult.OpId,
		Result: putResult.Value,
	}
	d.NotifyCh <- resultStruct
	d.removeOutstandingPut(putArgs)
	//go d.handlePutTimeout(putArgs) // M2: handle Put timeout
}

// Removes the put matching putArgs from outstanding puts
func (d *KVS) removeOutstandingPut(putArgs *util.PutArgs) {
	d.Mutex.Lock()
	outstandingPuts := d.Puts[putArgs.Key]
	for outstandingPuts.Len() > 0 {
		elem := outstandingPuts.Front()
		put := elem.Value.(util.PutArgs)
		if put.OpId == putArgs.OpId {
			outstandingPuts.Remove(elem)
			d.sendBufferedGets(put.Key, put.OpId)
		}
	}
	d.Mutex.Unlock()
}

// Adds a new outstanding put to a KVS
func (d *KVS) addOutstandingPut(key string, putArgs *util.PutArgs) {
	d.Mutex.Lock()
	_, exists := d.Puts[key]
	if !exists {
		d.Puts[key] = list.New()
		d.BufferedGets[key] = list.New()
	}
	d.Puts[key].PushBack(putArgs)
	d.Mutex.Unlock()
}

// Updates a KVS's estimated RTT based on an operation's RTT
func (d *KVS) updateInProgressAndRtt(opId uint8) {
	d.Mutex.Lock()
	newRtt := time.Now().Sub(d.InProgress[opId])
	d.RTT = (d.RTT + newRtt) / 2
	delete(d.InProgress, opId)
	d.Mutex.Unlock()
}
