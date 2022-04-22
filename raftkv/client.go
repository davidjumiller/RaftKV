package raftkv

import (
	"container/list"
	"cs.ubc.ca/cpsc416/p1/util"
	"errors"
	"fmt"
	"github.com/DistributedClocks/tracing"
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

type PutSend struct {
	ClientId string
	ServerId int
	OpId     uint8
	Key      string
	Value    string
}

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

type GetSend struct {
	ClientId string
	ServerId int
	OpId     uint8
	Key      string
}

type GetResultRecvd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type NewServerConnection struct {
	ClientId   string
	ServerId   int
	ServerAddr string
}

type Op struct {
	Trace *tracing.Trace
	Args  interface{}
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
	NotifyCh        NotifyChannel
	KTrace          *tracing.Trace
	ClientId        string
	ServerId        int // Index of server in ServerList we are connected to
	ServerList      []string
	Tracer          *tracing.Tracer
	KeyMutex        map[string]*sync.Mutex // A map of keys to mutexes
	OpMutex         *sync.Mutex
	IndexMutex      *sync.Mutex
	ConnectionMutex *sync.RWMutex
	QueuedOps       map[string]*list.List // A map of keys to queues. Operations are placed on these queues to ensure proper ordering
	OpId            uint8
	AliveCh         chan int
	Client          *rpc.Client
}

func NewKVS() *KVS {
	return &KVS{
		NotifyCh:        nil,
		ServerId:        0,
		KeyMutex:        make(map[string]*sync.Mutex),
		OpMutex:         new(sync.Mutex),
		IndexMutex:      new(sync.Mutex),
		ConnectionMutex: new(sync.RWMutex),
		QueuedOps:       make(map[string]*list.List),
		OpId:            1,
		AliveCh:         make(chan int),
	}
}

var timeout = 2 * time.Second

// Start Starts the instance of Client to use for connecting to the system.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the system, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, serverIPPortList []string, chCapacity int) (NotifyChannel, error) {
	d.NotifyCh = make(NotifyChannel, chCapacity)
	d.KTrace = localTracer.CreateTrace()
	d.ClientId = clientId
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
func (d *KVS) Get(key string) error {
	d.OpMutex.Lock()
	localOpId := d.OpId
	d.OpId = d.OpId + 1
	d.OpMutex.Unlock()

	getArgs, gTrace := d.createGetArgs(key, localOpId)
	d.queueOp(key, getArgs, gTrace)
	return nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value OpId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(key string, value string) error {
	localOpId := d.nextOpId()

	// Send put to head via RPC
	putArgs, pTrace := d.createPutArgs(key, value, localOpId)
	d.queueOp(key, putArgs, pTrace)
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
func (d *KVS) createPutArgs(key string, value string, localOpId uint8) (*util.PutArgs, *tracing.Trace) {
	// Start Put trace
	trace := d.Tracer.CreateTrace()
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
func (d *KVS) createGetArgs(key string, localOpId uint8) (*util.GetArgs, *tracing.Trace) {
	// Start Get trace
	trace := d.Tracer.CreateTrace()
	trace.RecordAction(GetStart{d.ClientId, localOpId, key})

	return &util.GetArgs{
			ClientId: d.ClientId,
			OpId:     localOpId,
			Key:      key,
			GToken:   nil,
		},
		trace
}

// Sends a Get request to a server and prepares to receive the result
func (d *KVS) sendGet(getArgs *util.GetArgs, trace *tracing.Trace) error {
	trace.RecordAction(GetSend{getArgs.ClientId, d.ServerId, getArgs.OpId, getArgs.Key})
	getArgs.GToken = trace.GenerateToken()

	getResult := &util.GetRes{
		ClientId: "",
		OpId:     0,
		Key:      "",
		Value:    "",
		GToken:   nil,
	}

	d.ConnectionMutex.RLock()
	defer d.ConnectionMutex.RUnlock()

	if d.Client == nil {
		return errors.New("Server Connection Dropped")
	}

	res := d.Client.Go("KVServer.Get", getArgs, getResult, nil)
	select {
	case call := <-res.Done:
		if call.Error != nil {
			// Server Error -- assume server has failed and try a different one
			return errors.New("Server Error")
		} else {
			// Successful reply
			d.getReceived(getResult)
			d.QueuedOps[getArgs.Key].Remove(d.QueuedOps[getArgs.Key].Front())
			return nil
		}
	case <-time.After(timeout):
		// Timeout -- assume server has failed and try a different one
		return errors.New("Server Timeout")
	}
}

func (d *KVS) getReceived(getResult *util.GetRes) {
	trace := d.Tracer.ReceiveToken(getResult.GToken)
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

// Sends a put to the server and waits for a result
func (d *KVS) sendPut(trace *tracing.Trace, localOpId uint8, putArgs *util.PutArgs) error {

	trace.RecordAction(PutSend{putArgs.ClientId, d.ServerId, putArgs.OpId, putArgs.Key, putArgs.Value})
	putArgs.PToken = trace.GenerateToken()

	putResult := &util.PutRes{
		ClientId: "",
		OpId:     0,
		Key:      "",
		Value:    "",
		PToken:   nil,
	}

	d.ConnectionMutex.RLock()
	defer d.ConnectionMutex.RUnlock()

	if d.Client == nil {
		return errors.New("Server Connection Dropped")
	}

	res := d.Client.Go("KVServer.Put", putArgs, putResult, nil)
	select {
	case call := <-res.Done:
		if call.Error != nil {
			// Server Error -- assume server has failed and try a different one
			return errors.New("Server Error")
		} else {
			// Successful reply
			d.putReceived(putResult)
			d.QueuedOps[putArgs.Key].Remove(d.QueuedOps[putArgs.Key].Front())
			return nil
		}
	case <-time.After(timeout):
		// Timeout -- assume server has failed and try a different one
		return errors.New("Server Timeout")
	}
}

func (d *KVS) putReceived(putResult *util.PutRes) {
	trace := d.Tracer.ReceiveToken(putResult.PToken)
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
}

// Sends result to client
func (d *KVS) sendResult(result *ResultStruct) {
	d.NotifyCh <- *result
}

// Processes the next queued operation for a specific key
func (d *KVS) nextOp(key string) {
	// Create a new mutex for this key if one doesnt exist
	if d.KeyMutex[key] == nil {
		d.KeyMutex[key] = new(sync.Mutex)
	}
	d.KeyMutex[key].Lock()
	// Process the next Op if there is one, do nothing otherwise
	if d.QueuedOps[key].Len() > 0 {
		elem := d.QueuedOps[key].Front()
		op, ok := elem.Value.(*Op)
		if !ok {
			return
		}
		// Check the type of operation
		if put, ok := op.Args.(*util.PutArgs); ok {
			// Send once, keep resending if there is an error
			err := d.sendPut(op.Trace, put.OpId, put)
			for err != nil {
				d.tryNextServer()
				err = d.sendPut(op.Trace, put.OpId, put)
			}
		} else if get, ok := op.Args.(*util.GetArgs); ok {
			// Send this get, and subsequent get requests
			d.sendGetGroup(elem, get, op, op.Trace)
		} else {
			// Error
			d.QueuedOps[key].Remove(elem)
		}
		// Send next request if one has been queued for that key
		go d.nextOp(key)
	}
	d.KeyMutex[key].Unlock()
}

// Send first get, and subsequent get requests concurrently
func (d *KVS) sendGetGroup(firstElem *list.Element, firstGet *util.GetArgs, op *Op, trace *tracing.Trace) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Send once, keep resending if there is an error
		err := d.sendGet(firstGet, op.Trace)
		for err != nil {
			d.tryNextServer()
			err = d.sendGet(firstGet, op.Trace)
		}
		wg.Done()
	}()
	lastOpGet := true
	elem := firstElem.Next()
	for elem != nil && lastOpGet == true {
		nextOp, ok := elem.Value.(*Op)
		if !ok {
			return
		}
		get, ok := nextOp.Args.(*util.GetArgs)
		lastOpGet = ok
		if ok {
			wg.Add(1)
			go func() {
				// Send once, keep resending if there is an error
				err := d.sendGet(get, nextOp.Trace)
				for err != nil {
					d.tryNextServer()
				}
				wg.Done()
			}()
		}
		elem = elem.Next()
	}
	wg.Wait()
}

// Places operation on the queue for its specific key
func (d *KVS) queueOp(key string, args interface{}, trace *tracing.Trace) {
	_, exists := d.QueuedOps[key]
	// Create new Queue if one doesnt exist
	if !exists {
		d.QueuedOps[key] = new(list.List)
	}
	op := &Op{
		Trace: trace,
		Args:  args,
	}
	d.QueuedOps[key].PushBack(op)
	// If queue was previously empty, immediately send next op
	if d.QueuedOps[key].Len() == 1 {
		go d.nextOp(key)
	}
}

func (d *KVS) nextOpId() uint8 {
	d.OpMutex.Lock()
	localOpId := d.OpId
	d.OpId = d.OpId + 1
	d.OpMutex.Unlock()
	return localOpId
}

func (d *KVS) connectToServer() error {
	d.IndexMutex.Lock()
	serverAddr := d.ServerList[d.ServerId]
	if d.Client != nil {
		d.Client.Close()
	}
	var err error
	d.Client, err = rpc.Dial("tcp", serverAddr)
	d.IndexMutex.Unlock()
	if err != nil {
		// Keep trying next server indefinitely
		fmt.Println("Could not connect to", serverAddr)
		return errors.New("Could not connect to server")
	} else {
		d.KTrace.RecordAction(NewServerConnection{d.ClientId, d.ServerId, serverAddr})
	}
	return nil
}

// attempts to connect to next server on the list
func (d *KVS) tryNextServer() {
	d.ConnectionMutex.Lock()
	defer d.ConnectionMutex.Unlock()

	d.IndexMutex.Lock()
	d.ServerId = (d.ServerId + 1) % len(d.ServerList)
	d.IndexMutex.Unlock()

	err := d.connectToServer()
	for err != nil {
		d.IndexMutex.Lock()
		d.ServerId = (d.ServerId + 1) % len(d.ServerList)
		d.IndexMutex.Unlock()
		err = d.connectToServer()
	}
}
