package raftkv

import (
	"net"
	"net/rpc"
	"github.com/DistributedClocks/tracing"
)

type GetArgs struct {
	Key		string
	GToken	tracing.TracingToken
}

type GetRes struct {
	Res		string
	GToken	tracing.TracingToken
}

type PutArgs struct {
	Key		string
	Value 	string
	PToken 	tracing.TracingToken
}

type PutRes struct {
	PToken	tracing.TracingToken
}

type KVServerConfig struct {
	// Values Read from config file
}

type KVServer struct {
	ServerAddr	string
	ServerList	[]string
	NumServers	uint8
	Raft		*raftkv.Raft
	Store 		map[string]string
}

func NewServer() *KVServer {
	return &KVServer{
		isLeader: false,
		serverList: []string{},
		log: []LogEntry{},
	}
}

func (kvs *KVServer) Start() error {
	return nil
}

func (kvs *KVServer) NewServerJoin() error {
	return nil
}

func (kvs *KVServer) Get(getArgs *GetArgs, getRes *GetRes) error {

	raftState := kvs.Raft.GetState()

	if raftState.IsLeader {
		raftState, err := kvs.Raft.Execute(getArgs.Key) // Arguments to be specified later
		if err != nil {
			return err
		}
		getRes.Res = kvs.Store[getArgs.Key]
	} else {
		conn, client, err := establishRPCConnection(kvs.ServerAddr, kvs.ServerList[raftState.LeaderId])
		if err != nil {
			return err
		}
		err = client.Call("KVServer.Get", getArgs, getRes) // Check if we can do it like this
		if err != nil {
			return err
		}
		client.close()
		conn.close()
	}

	// TODO: Tracing
	return nil
}

func (kvs *KVServer) Put(putArgs *PutArgs, putRes *PutRes) error {

	raftState := kvs.Raft.GetState()

	if raftState.IsLeader {
		raftState, err := kvs.Raft.Execute(putArgs.Key) // Arguments to be specified later
		if err != nil {
			return err
		}
		// Database updated from raft side via apply?
	} else {
		conn, client, err := establishRPCConnection(kvs.ServerAddr, kvs.ServerList[raftState.LeaderId])
		if err != nil {
			return err
		}
		err = client.Call("KVServer.Put", putArgs, putRes) // Check if we can do it like this, directly
		if err != nil {
			return err
		}
		client.close()
		conn.close()
	}

	// TODO: Tracing
	return nil
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