package raftkv

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"

	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
)

type PutRecvd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type PutResultFwd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type GetRecvd struct {
	ClientId string
	OpId     uint8
	Key      string
}

type GetFwd struct {
	ClientId string
	OpId     uint8
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type GetResultFwd struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
}

type ServerStart struct {
	ServerIdx int
}

type ServerListening struct {
	ServerIdx int
}

type KVServerConfig struct {
	ServerIdx int // this server's index into ServerList and RaftList

	// addresses on which of each server in the system listens for RPCs,
	// where this server's address is at index ServerIdx, i.e. ServerList[ServerIdx]
	ServerList []string

	// addresses on which each server's Raft instance listens for RPCs,
	// where this server's Raft instance is at index ServerIdx, i.e. RaftList[ServerIdx]
	RaftList []string

	// tracing config
	TracingServerAddr string
	TracingIdentity   string
	Secret            []byte
}

type KVServer struct {
	ServerIdx  int
	ServerList []string
	Raft       *Raft             // this server's Raft instance
	LastLdrID  int               // ID of the last known leader server
	Mutex      sync.Mutex        // Mutex lock for KVServer
	Client     *rpc.Client       // RPC Client for leader server
	ApplyCh    chan ApplyMsg     // channel to receive updates from Raft
	Store      map[string]string // in-memory key-value store
	Tracer     *tracing.Tracer

	// Puts currently being processed by Raft, where Puts are identified by
	// ClientId and OpId, as in OutstandingPuts[Put.ClientId][Put.OpId]
	OutstandingPuts map[string]*util.SafeUInt8Set
}

func NewServer() *KVServer {
	return &KVServer{
		Store:           make(map[string]string),
		OutstandingPuts: make(map[string]*util.SafeUInt8Set),
	}
}

type RemoteServer struct {
	KVServer *KVServer
}

func (kvs *KVServer) Start(serverIdx int, serverList []string, tracer *tracing.Tracer, raft *Raft) error {
	kvs.ServerIdx = serverIdx
	kvs.ServerList = serverList
	kvs.Tracer = tracer
	kvs.Raft = raft
	kvs.ApplyCh = raft.ApplyCh
	kvs.LastLdrID = -1

	// Begin Server trace
	trace := tracer.CreateTrace()
	trace.RecordAction(ServerStart{serverIdx})

	// Check raft state
	kvs.checkLeader()

	// Start listening for RPCs
	rpcServer := &RemoteServer{kvs}
	err := rpc.RegisterName("KVServer", rpcServer)
	if err != nil {
		fmt.Println("failed to register this server for RPCs")
		return err
	}
	_, err = util.StartRPCListener(serverList[serverIdx])
	if err != nil {
		fmt.Println("failed to start listening for RPCs")
		return err
	}
	trace.RecordAction(ServerListening{serverIdx})

	// Maintain local store with updates from Raft
	go kvs.updateStore()

	for {
		// Serve indefinitely
	}
	return nil
}

func (rs *RemoteServer) Get(getArgs *util.GetArgs, getRes *util.GetRes) error {
	kvs := rs.KVServer

	trace := kvs.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(GetRecvd{
		ClientId: getArgs.ClientId,
		OpId:     getArgs.OpId,
		Key:      getArgs.Key,
	})

	// Check if leader connection needs to be updated; drop request if connection can't be made
	err := kvs.checkLeader()
	if err != nil {
		fmt.Println("Connection to leader failed, dropping request")
		return err
	}

	if kvs.ServerIdx == kvs.LastLdrID {
		// Return Get response to caller
		value := kvs.Store[getArgs.Key]
		trace.RecordAction(GetResult{
			ClientId: getArgs.ClientId,
			OpId:     getArgs.OpId,
			Key:      getArgs.Key,
			Value:    value,
		})
		getRes.ClientId = getArgs.ClientId
		getRes.OpId = getArgs.OpId
		getRes.Key = getArgs.Key
		getRes.Value = value
		getRes.GToken = trace.GenerateToken()
	} else {
		// Forward Get request to leader
		trace.RecordAction(GetFwd{
			ClientId: getArgs.ClientId,
			OpId:     getArgs.OpId,
			Key:      getArgs.Key,
		})
		getArgs.GToken = trace.GenerateToken()
		err = kvs.Client.Call("KVServer.Get", getArgs, getRes)
		if err != nil {
			return err
		}

		// Forward Get response back to caller
		trace = kvs.Tracer.ReceiveToken(getRes.GToken)
		trace.RecordAction(GetResultFwd{
			ClientId: getRes.ClientId,
			OpId:     getArgs.OpId,
			Key:      getRes.Key,
			Value:    getRes.Value,
		})
		getRes.GToken = trace.GenerateToken()
	}

	return nil
}

func (rs *RemoteServer) Put(putArgs *util.PutArgs, putRes *util.PutRes) error {
	kvs := rs.KVServer

	trace := kvs.Tracer.ReceiveToken(putArgs.PToken)
	trace.RecordAction(PutRecvd{
		ClientId: putArgs.ClientId,
		OpId:     putArgs.OpId,
		Key:      putArgs.Key,
		Value:    putArgs.Value,
	})

	// Check if leader connection needs to be updated; drop request if connection can't be made
	err := kvs.checkLeader()
	if err != nil {
		fmt.Println("Connection to leader failed, dropping request")
		return err
	}

	if kvs.ServerIdx == kvs.LastLdrID {
		// Execute (log) Put request on Raft
		kvs.addOutstandingPut(putArgs)
		token := kvs.Raft.Execute(util.RaftPutReq{putArgs.ClientId, putArgs.Key, putArgs.Value, putArgs.OpId}, trace.GenerateToken())
		trace = kvs.Tracer.ReceiveToken(token)

		// Wait for Raft to finish logging Put
		for kvs.OutstandingPuts[putArgs.ClientId].Has(putArgs.OpId) {
		}

		// Return Put response to caller
		value := kvs.Store[putArgs.Key]
		trace.RecordAction(PutResult{
			ClientId: putArgs.ClientId,
			OpId:     putArgs.OpId,
			Key:      putArgs.Key,
			Value:    value,
		})
		putRes.ClientId = putArgs.ClientId
		putRes.OpId = putArgs.OpId
		putRes.Key = putArgs.Key
		putRes.Value = value
		putRes.PToken = trace.GenerateToken()
	} else {
		// Forward Put request to leader
		trace.RecordAction(PutFwd{
			ClientId: putArgs.ClientId,
			OpId:     putArgs.OpId,
			Key:      putArgs.Key,
			Value:    putArgs.Value,
		})
		putArgs.PToken = trace.GenerateToken()
		err = kvs.Client.Call("KVServer.Put", putArgs, putRes)
		if err != nil {
			return err
		}

		// Forward Put response back to caller
		trace = kvs.Tracer.ReceiveToken(putRes.PToken)
		trace.RecordAction(PutResultFwd{
			ClientId: putRes.ClientId,
			OpId:     putRes.OpId,
			Key:      putRes.Key,
			Value:    putRes.Value,
		})
		putRes.PToken = trace.GenerateToken()
	}

	return nil
}

func (kvs *KVServer) addOutstandingPut(putArgs *util.PutArgs) {
	_, ok := kvs.OutstandingPuts[putArgs.ClientId]
	if !ok {
		kvs.OutstandingPuts[putArgs.ClientId] = util.NewSafeUInt8Set()
	}
	kvs.OutstandingPuts[putArgs.ClientId].Add(putArgs.OpId)
}

// Update store with state changes notified by Raft via ApplyCh
func (kvs *KVServer) updateStore() {
	for applyMsg := range kvs.Raft.ApplyCh {
		putArgs, ok := applyMsg.Command.(util.RaftPutReq)
		if ok {
			// Command is Put; update store
			kvs.Store[putArgs.Key] = putArgs.Value

			if kvs.ServerIdx == kvs.LastLdrID {
				// Remove outstanding Put to signal that this Put may be returned
				kvs.OutstandingPuts[putArgs.ClientId].Remove(putArgs.OpId)
			}
		}
	}
}

func (kvs *KVServer) checkLeader() error {
	/* Locking to prevent the case where util.TryMakeClient() is called more than once simultaneously,
	leading to only one get/put succeeding and the rest dropping their requests */
	kvs.Mutex.Lock()
	defer kvs.Mutex.Unlock()

	raftState := kvs.Raft.GetState()

	if raftState.LeaderID == -1 {
		// Leader is unavailable
		kvs.closeRPCClient()
		kvs.LastLdrID = raftState.LeaderID
		return errors.New("no leader elected")
	}
	if raftState.LeaderID == kvs.LastLdrID {
		// Leader remains unchanged
		return nil
	}

	// Leader has changed
	kvs.closeRPCClient()
	if raftState.LeaderID != kvs.ServerIdx {
		// Another server is the leader, so this server needs to make new connection to the leader
		var err error
		kvs.Client, err = rpc.Dial("tcp", kvs.ServerList[raftState.LeaderID])
		if err != nil {
			return err
		}
	}
	kvs.LastLdrID = raftState.LeaderID
	return nil
}

// Close RPC Client for connection to leader
// Call only while lock is held
func (kvs *KVServer) closeRPCClient() {
	if kvs.Client != nil {
		kvs.Client.Close()
		kvs.Client = nil
	}
}
