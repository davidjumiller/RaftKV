package raftkv

import (
	"cs.ubc.ca/cpsc416/p1/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net/rpc"
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

type ServerStart struct {
	ServerId int
}

type ServerListening struct {
	ServerId int
}

type KVServerConfig struct {
	ServerId          int      // this server's ID; used to index into ServersList
	ServerAddr        string   // address from which this server sends RPCs
	ServerListenAddr  string   // address on which this server listens for RPCs
	RaftListenAddr    string   // addresses of all possible servers in the system
	ServerList        []string // Currently, Index = ServerId
	RaftList          []string // Also Index = ServerId
	NumServers        uint8
	TracingServerAddr string
	TracingIdentity   string
	Secret            []byte
}

type KVServer struct {
	ServerId   int
	ServerAddr string
	ServerList []string
	Raft       *Raft             // this server's Raft instance
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

func (kvs *KVServer) Start(serverId int, serverAddr string, serverListenAddr string, serverList []string, tracer *tracing.Tracer, raft *Raft) error {
	kvs.ServerId = serverId
	kvs.ServerAddr = serverAddr
	kvs.ServerList = serverList
	kvs.Tracer = tracer
	kvs.Raft = raft
	kvs.ApplyCh = raft.applyCh

	// Begin Server trace
	trace := tracer.CreateTrace()
	trace.RecordAction(ServerStart{serverId})

	// Start listening for RPCs
	rpcServer := &RemoteServer{kvs}
	err := rpc.RegisterName("KVServer", rpcServer)
	if err != nil {
		fmt.Println("failed to register this server for RPCs")
		return err
	}
	_, err = util.StartRPCListener(serverListenAddr)
	if err != nil {
		fmt.Println("failed to start listening for RPCs")
		return err
	}
	trace.RecordAction(ServerListening{serverId})

	for {
		// Serve indefinitely
	}
	return nil
}

func (rs *RemoteServer) Get(getArgs *util.GetArgs, getRes *util.GetRes) error {

	kvs := rs.KVServer
	// raftState := kvs.Raft.GetState()
	leaderId := 1 // leaderId := raftState.LeaderID

	trace := kvs.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(GetRecvd{
		ClientId: getArgs.ClientId,
		Key:      getArgs.Key,
	})

	if kvs.ServerId == leaderId {
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
		conn, client := util.MakeClient("", kvs.ServerList[leaderId])
		trace.RecordAction(GetFwd{
			ClientId: getArgs.ClientId,
			Key:      getArgs.Key,
		})
		getArgs.GToken = trace.GenerateToken()
		err := client.Call("KVServer.Get", getArgs, getRes)
		if err != nil {
			return err
		}
		client.Close()
		conn.Close()
	}

	return nil
}

func (rs *RemoteServer) Put(putArgs *util.PutArgs, putRes *util.PutRes) error {

	kvs := rs.KVServer
	// raftState := kvs.Raft.GetState()
	leaderId := 1 // leaderId := raftState.LeaderID

	trace := kvs.Tracer.ReceiveToken(putArgs.PToken)
	trace.RecordAction(PutRecvd{
		ClientId: putArgs.ClientId,
		Key:      putArgs.Key,
		Value:    putArgs.Value,
	})

	if kvs.ServerId == leaderId {
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
		conn, client := util.MakeClient("", kvs.ServerList[leaderId])
		trace.RecordAction(PutFwd{
			ClientId: putArgs.ClientId,
			Key:      putArgs.Key,
			Value:    putArgs.Value,
		})
		putArgs.PToken = trace.GenerateToken()
		err := client.Call("KVServer.Put", putArgs, putRes)
		if err != nil {
			return err
		}
		client.Close()
		conn.Close()
	}

	return nil
}
