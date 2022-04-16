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

type PutStart struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type PutSend PutStart

type PutResultRecvd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type GetStart struct {
	ClientId string
	OpId     uint8
	Key      string
}

type GetSend GetStart

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

type NewServerConnection struct {
	ClientId   string
	ServerAddr string
}

// NotifyChannel is used for notifying the client about a result for an operation.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint8
	Type   string
	Key    string
	Result string
}

type KVS struct {
	NotifyCh NotifyChannel
	// Add more KVS instance state here.
	KTrace          *tracing.Trace
	ClientId        string
	LocalServerAddr string
	RemoteAddrIndex int // Index of server in ServerList we are connected to
	ServerList      []string
	RTT             time.Duration
	Tracer          *tracing.Tracer
	InProgress      map[uint8]time.Time // Map representing sent requests that haven't been responded to
	PutMutex        *sync.Mutex
	GetMutex        *sync.Mutex
	RTTMutex        *sync.Mutex
	OpMutex         *sync.Mutex
	IndexMutex      *sync.Mutex
	Puts            map[string]*list.List // list of outstanding put ids for a key
	BufferedGets    map[string]*list.List
	OpId            uint8
	AliveCh         chan int
	Conn            *net.TCPConn
	Client          *rpc.Client
}

func NewKVS() *KVS {
	return &KVS{
		NotifyCh:        nil,
		RemoteAddrIndex: 0,
		InProgress:      make(map[uint8]time.Time),
		PutMutex:        new(sync.Mutex),
		GetMutex:        new(sync.Mutex),
		RTTMutex:        new(sync.Mutex),
		OpMutex:         new(sync.Mutex),
		IndexMutex:      new(sync.Mutex),
		Puts:            make(map[string]*list.List),
		BufferedGets:    make(map[string]*list.List),
		OpId:            1,
		RTT:             3 * time.Second,
		AliveCh:         make(chan int),
	}
}

var timeout = time.Second

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
	d.connectToServer()
	d.KTrace.RecordAction(KvslibStart{clientId})
	return d.NotifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value OpId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, key string) error {
	d.lockLog("check outstanding puts", d.PutMutex)
	outstandingPuts, exists := d.Puts[key]
	numPuts := 0
	if exists {
		numPuts = outstandingPuts.Len()
	}
	d.unlockLog("check outstanding puts", d.PutMutex)
	d.lockLog("op", d.OpMutex)
	localOpId := d.OpId
	d.OpId = d.OpId + 1
	d.unlockLog("op", d.OpMutex)

	getArgs := d.createGetArgs(tracer, key, localOpId)
	if numPuts > 0 {
		d.lockLog("last outstanding put", d.PutMutex)
		elem := outstandingPuts.Back()
		put := elem.Value.(*util.PutArgs)
		bufferedGet := &BufferedGet{
			Args:    getArgs,
			PutOpId: put.OpId,
		}
		d.unlockLog("last outstanding put", d.PutMutex)
		d.lockLog("add buffered get", d.GetMutex)
		d.BufferedGets[key].PushBack(bufferedGet)
		d.unlockLog("add buffered get", d.GetMutex)
	} else {
		d.sendGet(getArgs)
	}
	return nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, key string, value string) error {
	localOpId := d.nextOpId()

	// Send put to head via RPC
	putArgs, pTrace := d.createPutArgs(tracer, key, value, localOpId)
	d.addOutstandingPut(key, putArgs)
	go d.sendPut(localOpId, putArgs, pTrace)
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
	util.CheckErr(err, "Could not close KVS client")
	go d.closeRoutines()
	return
}

func (d *KVS) closeRoutines() {
	if len(d.AliveCh) > 0 {
		d.AliveCh <- 1
	}
	close(d.AliveCh)
}

// Creates PutArgs struct for a new Put
func (d *KVS) createPutArgs(tracer *tracing.Tracer, key string, value string, localOpId uint8) (*util.PutArgs, *tracing.Trace) {
	// Start Put trace
	trace := tracer.CreateTrace()
	trace.RecordAction(PutStart{d.ClientId, localOpId, key, value})

	return &util.PutArgs{
		ClientId: d.ClientId,
		OpId:     localOpId,
		Key:      key,
		Value:    value,
		PToken:   nil,
	}, trace
}

// Creates GetArgs struct for a new Get
func (d *KVS) createGetArgs(tracer *tracing.Tracer, key string, localOpId uint8) *util.GetArgs {
	// Start Get trace
	trace := tracer.CreateTrace()
	trace.RecordAction(GetStart{d.ClientId, localOpId, key})

	return &util.GetArgs{
		ClientId: d.ClientId,
		OpId:     localOpId,
		Key:      key,
		GToken:   trace.GenerateToken(),
	}
}

// Sends a Get request to a server and prepares to receive the result
func (d *KVS) sendGet(getArgs *util.GetArgs) {
	d.lockLog("rtt", d.RTTMutex)
	d.InProgress[getArgs.OpId] = time.Now()
	d.unlockLog("rtt", d.RTTMutex)

	trace := d.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(GetSend{getArgs.ClientId, getArgs.OpId, getArgs.Key})
	getArgs.GToken = trace.GenerateToken()

	getResult := &util.GetRes{
		ClientId: "",
		OpId:     0,
		Key:      "",
		Value:    "",
		GToken:   nil,
	}
	d.Client.Go("KVServer.Get", getArgs, getResult, nil)
	<-time.After(timeout)
	if getResult.ClientId == getArgs.ClientId && getResult.OpId == getArgs.OpId {
		// Successful reply
		d.getReceived(trace, getResult)
	} else {
		d.tryNextServer()
		d.sendGet(getArgs)
	}
}

func (d *KVS) getReceived(trace *tracing.Trace, getResult *util.GetRes) {
	trace = d.Tracer.ReceiveToken(getResult.GToken)
	trace.RecordAction(GetResultRecvd{
		ClientId: getResult.ClientId,
		OpId:     getResult.OpId,
		Key:      getResult.Key,
		Value:    getResult.Value,
	})
	resultStruct := &ResultStruct{
		OpId:   getResult.OpId,
		Type:   "Get",
		Key:    getResult.Key,
		Result: getResult.Value,
	}
	go d.sendResult(resultStruct)
}

// Sends the buffered Gets to the server matching the given key and opId
func (d *KVS) sendBufferedGets(key string, putOpId uint8) {
	bufferedGets := d.BufferedGets[key]
	elem := bufferedGets.Front()
	for elem != nil {
		bufferedGet := elem.Value.(*BufferedGet)
		if bufferedGet.PutOpId == putOpId {
			getToSend := elem
			elem = elem.Next()
			d.lockLog("send buffered get", d.GetMutex)
			bufferedGets.Remove(getToSend)
			d.unlockLog("send buffered get", d.GetMutex)
			d.sendGet(bufferedGet.Args)
		} else {
			elem = elem.Next()
		}
	}
}

// Sends a put to the server and waits for a result
func (d *KVS) sendPut(localOpId uint8, putArgs *util.PutArgs, pTrace *tracing.Trace) {
	d.lockLog("rtt", d.RTTMutex)
	d.InProgress[localOpId] = time.Now()
	d.unlockLog("rtt", d.RTTMutex)

	pTrace.RecordAction(PutSend{putArgs.ClientId, putArgs.OpId, putArgs.Key, putArgs.Value})
	putArgs.PToken = pTrace.GenerateToken()

	putResult := &util.PutRes{
		ClientId: "",
		OpId:     0,
		Key:      "",
		Value:    "",
		PToken:   nil,
	}
	d.Client.Go("KVServer.Put", putArgs, putResult, nil)
	<-time.After(timeout)
	if putResult.ClientId == putArgs.ClientId && putResult.OpId == putArgs.OpId {
		// Successful reply
		d.putReceived(pTrace, putResult, putArgs)
	} else {
		d.tryNextServer()
		d.sendPut(localOpId, putArgs, pTrace)
	}
}

func (d *KVS) putReceived(trace *tracing.Trace, putResult *util.PutRes, putArgs *util.PutArgs) {
	trace = d.Tracer.ReceiveToken(putResult.PToken)
	trace.RecordAction(PutResultRecvd{
		ClientId: putResult.ClientId,
		OpId:     putResult.OpId,
		Key:      putResult.Key,
		Value:    putResult.Value,
	})
	resultStruct := &ResultStruct{
		OpId:   putResult.OpId,
		Type:   "Put",
		Key:    putResult.Key,
		Result: putResult.Value,
	}
	go d.sendResult(resultStruct)
	d.removeOutstandingPut(putArgs)
}

// Removes the put matching putArgs from outstanding puts
func (d *KVS) removeOutstandingPut(putArgs *util.PutArgs) {
	d.lockLog("remove outstanding put", d.PutMutex)
	outstandingPuts := d.Puts[putArgs.Key]
	elem := outstandingPuts.Front()
	for elem != nil {
		put := elem.Value.(*util.PutArgs)
		if put.OpId == putArgs.OpId {
			outstandingPut := elem
			elem = elem.Next()
			outstandingPuts.Remove(outstandingPut)
			d.sendBufferedGets(put.Key, put.OpId)
		} else {
			elem = elem.Next()
		}
	}
	d.unlockLog("remove outstanding put", d.PutMutex)
}

// Adds a new outstanding put to a KVS
func (d *KVS) addOutstandingPut(key string, putArgs *util.PutArgs) {
	d.lockLog("add outstanding put", d.PutMutex)
	_, exists := d.Puts[key]
	if !exists {
		d.Puts[key] = new(list.List)
		d.BufferedGets[key] = new(list.List)
	}
	d.Puts[key].PushBack(putArgs)
	d.unlockLog("add outstanding put", d.PutMutex)

}

// Updates a KVS's estimated RTT based on an operation's RTT
func (d *KVS) updateInProgressAndRtt(opId uint8) {
	newRtt := time.Now().Sub(d.InProgress[opId])
	d.RTT = (d.RTT + newRtt) / 2
	d.lockLog("rtt", d.RTTMutex)
	delete(d.InProgress, opId)
	d.unlockLog("rtt", d.RTTMutex)
}

// Sends result to client
func (d *KVS) sendResult(result *ResultStruct) {
	d.NotifyCh <- *result
}

func (d *KVS) lockLog(lockname string, lock *sync.Mutex) {
	lock.Lock()
	//fmt.Println(lockname, "lock acquired") // for debugging purposes
}

func (d *KVS) unlockLog(lockname string, lock *sync.Mutex) {
	lock.Unlock()
	//fmt.Println(lockname, "lock released") // for debugging purposes
}

func (d *KVS) nextOpId() uint8 {
	d.lockLog("put", d.PutMutex)
	localOpId := d.OpId
	d.OpId = d.OpId + 1
	d.unlockLog("put", d.PutMutex)
	return localOpId
}

func (d *KVS) connectToServer() {
	d.lockLog("next server", d.IndexMutex)
	serverAddr := d.ServerList[d.RemoteAddrIndex]
	if d.Client != nil {
		d.Client.Close()
	}
	if d.Conn != nil {
		d.Conn.Close()
	}
	var err error
	d.Conn, d.Client, err = util.TryMakeClient(d.LocalServerAddr, serverAddr)
	d.unlockLog("next server", d.IndexMutex)
	if err != nil {
		// Keep trying next server indefinitely
		util.CheckErr(err, "hello???", err)
		d.tryNextServer()
	} else {
		trace := d.Tracer.CreateTrace()
		trace.RecordAction(NewServerConnection{d.ClientId, serverAddr})
	}
}

// attempts to connect to next server on the list
func (d *KVS) tryNextServer() {
	//d.lockLog("next server index", d.IndexMutex)
	//d.RemoteAddrIndex += 1
	//if d.RemoteAddrIndex >= len(d.ServerList) {
	//	d.RemoteAddrIndex = 0
	//}
	//d.unlockLog("next server index", d.IndexMutex)
	//d.connectToServer()
}
