package raftkv

import (
	"cs.ubc.ca/cpsc416/p1/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"sync"
)

type PutRecvd struct {
	ClientId string
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	Key      string
	Value    string
}

type PutFwdRecvd struct {
	ClientId string
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	Key      string
	Value    string
}

type PutResultFwd PutResult

type GetRecvd struct {
	ClientId string
	Key      string
}

type GetFwd struct {
	ClientId string
	Key      string
}

type GetResult struct {
	ClientId string
	Key      string
	Value    string
}

type GetResultFwd GetResult

type ServerStart struct {
	ServerIdx int
}

type ServerListening struct {
	ServerIdx int
}

type KVServerConfig struct {
	ServerIdx  int    // this server's index into ServerList and RaftList
	ServerAddr string // address from which this server sends RPCs

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
	ServerAddr string
	ServerList []string
	Raft       *Raft             // this server's Raft instance
	LastLdrID  int               // This is the ID of the last known leader server
	Mutex      sync.Mutex        // Mutex lock for KVServer
	Conn       *net.TCPConn      // TCP Connection to leader server
	Client     *rpc.Client       // RPC Client for leader server
	ApplyCh    chan ApplyMsg     // channel to receive updates from Raft
	Store      map[string]string // in-memory key-value store
	Tracer     *tracing.Tracer
}

func NewServer() *KVServer {
	return &KVServer{
		ServerList: []string{},
		Store:      make(map[string]string),
	}
}

type RemoteServer struct {
	KVServer *KVServer
}

func (kvs *KVServer) Start(serverIdx int, serverAddr string, serverList []string, tracer *tracing.Tracer, raft *Raft) error {
	kvs.ServerIdx = serverIdx
	kvs.ServerAddr = serverAddr
	kvs.ServerList = serverList
	kvs.Tracer = tracer
	kvs.Raft = raft
	kvs.ApplyCh = raft.applyCh
	kvs.LastLdrID = -1

	// Begin Server trace
	trace := tracer.CreateTrace()
	trace.RecordAction(ServerStart{serverIdx})

	// Check raft state
	kvs.checkLeader(kvs.Raft.GetState())

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

	for {
		// Serve indefinitely
	}
	return nil
}

func (rs *RemoteServer) Get(getArgs *util.GetArgs, getRes *util.GetRes) error {

	kvs := rs.KVServer
	raftState := kvs.Raft.GetState()
	// Check if leader connection needs to be updated, if connection can't be made, drop this request
	err := kvs.checkLeader(raftState)
	if err != nil {
		fmt.Println("Connection to leader failed, dropping request")
		return err
	}

	trace := kvs.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(GetRecvd{
		ClientId: getArgs.ClientId,
		Key:      getArgs.Key,
	})

	if kvs.ServerIdx == kvs.LastLdrID {
		err := kvs.Raft.Execute(getArgs.Key) // Arguments to be specified later
		if err != nil {
			return err
		}
		val := kvs.Store[getArgs.Key]
		trace.RecordAction(GetResult{
			ClientId: getArgs.ClientId,
			Key:      getArgs.Key,
			Value:    kvs.Store[getArgs.Key],
		})
		getRes.ClientId = getArgs.ClientId
		getRes.OpId = getArgs.OpId
		getRes.Key = getArgs.Key
		getRes.Value = val
		getRes.GToken = trace.GenerateToken()
	} else {
		trace.RecordAction(GetFwd{
			ClientId: getArgs.ClientId,
			Key:      getArgs.Key,
		})
		getArgs.GToken = trace.GenerateToken()
		err := kvs.Client.Call("KVServer.Get", getArgs, getRes)
		if err != nil {
			return err
		}
		trace = kvs.Tracer.ReceiveToken(getRes.GToken)
		trace.RecordAction(GetResultFwd{
			ClientId: getRes.ClientId,
			Key:      getRes.Key,
			Value:    getRes.Value,
		})
		getRes.GToken = trace.GenerateToken()
	}

	return nil
}

func (rs *RemoteServer) Put(putArgs *util.PutArgs, putRes *util.PutRes) error {

	kvs := rs.KVServer
	raftState := kvs.Raft.GetState()
	// Check if leader connection needs to be updated, if connection can't be made, drop this request
	err := kvs.checkLeader(raftState)
	if err != nil {
		fmt.Println("Connection to leader failed, dropping request")
		return err
	}

	trace := kvs.Tracer.ReceiveToken(putArgs.PToken)
	trace.RecordAction(PutRecvd{
		ClientId: putArgs.ClientId,
		Key:      putArgs.Key,
		Value:    putArgs.Value,
	})

	if kvs.ServerIdx == kvs.LastLdrID {
		err := kvs.Raft.Execute(putArgs.Key) // Arguments to be specified later
		if err != nil {
			return err
		}
		kvs.Store[putArgs.Key] = putArgs.Value // Database updated from raft side via apply in the future
		trace.RecordAction(PutResult{
			ClientId: putArgs.ClientId,
			Key:      putArgs.Key,
			Value:    kvs.Store[putArgs.Key],
		})
		putRes.ClientId = putArgs.ClientId
		putRes.OpId = putArgs.OpId
		putRes.Key = putArgs.Key
		putRes.Value = putArgs.Value
		putRes.PToken = trace.GenerateToken()
	} else {
		trace.RecordAction(PutFwd{
			ClientId: putArgs.ClientId,
			Key:      putArgs.Key,
			Value:    putArgs.Value,
		})
		putArgs.PToken = trace.GenerateToken()
		err := kvs.Client.Call("KVServer.Put", putArgs, putRes)
		if err != nil {
			return err
		}
		trace = kvs.Tracer.ReceiveToken(putRes.PToken)
		trace.RecordAction(PutResultFwd{
			ClientId: putRes.ClientId,
			Key:      putRes.Key,
			Value:    putRes.Value,
		})
		putRes.PToken = trace.GenerateToken()
	}

	return nil
}

func (kvs *KVServer) checkLeader(raftState RaftState) error {
	/* Locking to prevent the case where util.TryMakeClient() is called more than once simultaneously,
	leading to only one get/put succeeding and the rest dropping their requests */
	kvs.Mutex.Lock()
	defer kvs.Mutex.Unlock()

	// Case where server is the leader
	if kvs.ServerIdx == raftState.LeaderID {
		kvs.Conn = nil
		kvs.Client = nil
		kvs.LastLdrID = raftState.LeaderID
		return nil

		// Case where server needs to make new connection to the leader
	} else if kvs.LastLdrID != raftState.LeaderID {
		if kvs.LastLdrID != kvs.ServerIdx {
			if kvs.Client != nil {
				kvs.Client.Close()
				kvs.Conn.Close()
			}
		}
		conn, client, err := util.TryMakeClient(kvs.ServerAddr, kvs.ServerList[raftState.LeaderID])
		if err != nil {
			return err
		}
		kvs.Conn = conn
		kvs.Client = client
		kvs.LastLdrID = raftState.LeaderID
	}
	return nil
}
