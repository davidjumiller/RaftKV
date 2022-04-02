package raftkv

import (
	"cs.ubc.ca/cpsc416/p1/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
)

type ServerStart struct {
	ServerId int
}

type ServerListening struct {
	ServerId int
}

type GetArgs struct {
	Key    string
	OpId   uint8
	GToken tracing.TracingToken
}

type GetRes struct {
	Value  string
	GToken tracing.TracingToken
}

type PutArgs struct {
	Key    string
	Value  string
	OpId   uint8
	PToken tracing.TracingToken
}

type PutRes struct {
	Key    string
	Value  string
	PToken tracing.TracingToken
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

func (kvs *KVServer) Get(getArgs *GetArgs, getRes *GetRes) error {

	raftState := kvs.Raft.GetState()

	if raftState.IsLeader {
		// Log entry in Raft
		_, err := kvs.Raft.Execute(getArgs.Key) // TODO: Arguments to be specified later
		if err != nil {
			return err
		}
		// Return value stored at key
		getRes.Value = kvs.Store[getArgs.Key]
	} else {
		// Forward request to leader
		conn, client, err := establishRPCConnection(kvs.ServerAddr, kvs.ServerList[raftState.LeaderId])
		if err != nil {
			return err
		}
		err = client.Call("KVServer.Get", getArgs, getRes)
		if err != nil {
			return err
		}
		client.Close()
		conn.Close()
	}

	// TODO: Tracing
	return nil
}

func (kvs *KVServer) Put(putArgs *PutArgs, putRes *PutRes) error {

	raftState := kvs.Raft.GetState()

	if raftState.IsLeader {
		// Log entry in Raft
		_, err := kvs.Raft.Execute(putArgs.Key) // TODO: Arguments to be specified later
		if err != nil {
			return err
		}
		// Return response to notify successful Put
		putRes.Value = putArgs.Value
	} else {
		// Forward request to leader
		conn, client, err := establishRPCConnection(kvs.ServerAddr, kvs.ServerList[raftState.LeaderId])
		if err != nil {
			return err
		}
		err = client.Call("KVServer.Put", putArgs, putRes)
		if err != nil {
			return err
		}
		client.Close()
		conn.Close()
	}

	// TODO: Tracing
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
