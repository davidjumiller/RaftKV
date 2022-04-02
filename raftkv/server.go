package raftkv

import (
	"cs.ubc.ca/cpsc416/p1/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
)

type PutRecvd struct {
	ClientId 	string
	Key			string
	Value		string
}

type PutFwd struct {
	ClientId 	string
	Key			string
	Value		string
}

type PutFwdRecvd struct {
	ClientId 	string
	Key			string
	Value		string
}

type PutResult struct {
	ClientId 	string
	Key			string
	Value		string
}

type GetRecvd struct {
	ClientId 	string
	Key			string
}

type GetFwd struct {
	ClientId 	string
	Key			string
}

type GetResult struct {
	ClientId 	string
	Key			string
	Value		string
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

	// Maintain local store with updates from Raft
	go kvs.updateStore()

	for {
		// Serve indefinitely
	}
	return nil
}

func (rs *RemoteServer) Get(getArgs *GetArgs, getRes *GetRes) error {

	kvs := rs.KVServer
	// raftState := kvs.Raft.GetState()

	trace := kvs.Tracer.ReceiveToken(getArgs.GToken)
	trace.RecordAction(GetRecvd{
		ClientId: "1" /*getArgs.ClientId*/,
		Key: getArgs.Key,
	})

	if kvs.ServerId == 1 /* raftState.IsLeader */ {
    // Log entry in Raft
		err := kvs.Raft.Execute(getArgs.Key) // TODO: Arguments to be specified later
		if err != nil {
			return err
		}
    // Return value stored at key
		val := kvs.Store[getArgs.Key]
		getRes.OpId = getArgs.OpId
		getRes.Key = getArgs.Key
		getRes.Value = val
		getRes.GToken = getArgs.GToken
		trace.RecordAction(GetResult{
			ClientId: "1" /*getArgs.ClientId*/,
			Key: getArgs.Key,
			Value: kvs.Store[getArgs.Key],
		})
	} else {
    // Forward request to leader
		conn, client, err := establishRPCConnection(kvs.ServerAddr, kvs.ServerList[1 /* raftState.LeaderId */])
		if err != nil {
			return err
		}
		trace.RecordAction(GetFwd{
			ClientId: "1" /*getArgs.ClientId*/,
			Key: getArgs.Key,
		})
		err = client.Call("KVServer.Get", getArgs, getRes) // Check if we can do it like this
		if err != nil {
			return err
		}
		client.Close()
		conn.Close()
	}

	// TODO: GetFwdRecvd Tracing
	return nil
}

func (rs *RemoteServer) Put(putArgs *PutArgs, putRes *PutRes) error {

	kvs := rs.KVServer
	// raftState := kvs.Raft.GetState()

	trace := kvs.Tracer.ReceiveToken(putArgs.PToken)
	trace.RecordAction(PutRecvd{
		ClientId: "1" /*putArgs.ClientId*/,
		Key: putArgs.Key,
		Value: putArgs.Value,
	})

	if kvs.ServerId == 1 /* raftState.IsLeader */{
    // Log entry in Raft
		err := kvs.Raft.Execute(putArgs.Key) // TODO: Arguments to be specified later
		if err != nil {
			return err
		}
    // Return response to notify successful Put
		kvs.Store[putArgs.Key] = putArgs.Value // Database updated from raft side via apply in the future
		putRes.OpId = putArgs.OpId
		putRes.Key = putArgs.Key
		putRes.Value = putArgs.Value
		putRes.PToken = putArgs.PToken
		trace.RecordAction(PutResult{
			ClientId: "1" /*putArgs.ClientId*/,
			Key: putArgs.Key,
			Value: kvs.Store[putArgs.Key],
		})
	} else {
    // Forward request to leader
		conn, client, err := establishRPCConnection(kvs.ServerAddr, kvs.ServerList[0 /* raftState.LeaderId */])
		if err != nil {
			return err
		}
		trace.RecordAction(PutFwd{
			ClientId: "1" /*putArgs.ClientId*/,
			Key: putArgs.Key,
			Value: putArgs.Value,
		})
		err = client.Call("KVServer.Put", putArgs, putRes) // Check if we can do it like this, directly
		if err != nil {
			return err
		}
		client.Close()
		conn.Close()
	}

	// TODO: PutFwdRecvd Tracing
	return nil
}

// Update store with state changes notified by Raft via ApplyCh
func (kvs *KVServer) updateStore() {
	for {
		applyMsg := <-kvs.Raft.applyCh
		putArgs, ok := applyMsg.Command.(PutArgs)
		if ok {
			// Command is Put; update store
			kvs.Store[putArgs.Key] = putArgs.Value
		}
	}
}

func establishRPCConnection(laddr, raddr string) (*net.TCPConn, *rpc.Client, error) {
	// Code adapted from Piazza post @471_f1, and our a3 code
	resolvedLaddr, err := net.ResolveTCPAddr("tcp", laddr)
	if err != nil {
		return nil, nil, err
	}
	resolvedRaddr, err := net.ResolveTCPAddr("tcp", raddr)
	if err != nil {
		return nil, nil, err
	}
	conn, err := net.DialTCP("tcp", resolvedLaddr, resolvedRaddr)
	if err != nil {
		return nil, nil, err
	}
	client := rpc.NewClient(conn)
	return conn, client, nil
}
