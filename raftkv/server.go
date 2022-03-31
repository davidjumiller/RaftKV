package raftkv

import (
	"net"
	"net/rpc"
)

type GetArgs struct {

}

type GetRes struct {
	res		string
}

type PutArgs struct {

}

type PutRes struct {

}

type LogEntry struct {

}

type KVServerConfig struct {
	// Values Read from config file
}

type KVServer struct {
	serverAddr	string
	serverList	[]string
	log 		[]LogEntry
	numServers	uint8
	raft		*raftkv.Raft
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

	if kvs.raft.isLeader {
		getRes.res = readDatabase(/* key */)
	} else {
		conn, client, err := establishRPCConnection(kvs.serverAddr, kvs.raft.leaderServerAddr) // Depends on raft implementation, maybe call GetState?
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

	return nil
}

func (kvs *KVServer) Put(putArgs *PutArgs, putRes *PutRes) error {

	if kvs.raft.isLeader {
		err := kvs.raft.updateLog(/* Log entry */) // Needs to be added to raft implementation
		if err != nil {
			return err
		}
		updateDatabase(/* KV entry */)
	} else {
		conn, client, err := establishRPCConnection(kvs.serverAddr, kvs.raft.leaderServerAddr) // Depends on raft implementation, maybe call GetState?
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

	return nil
}

func updateDatabase(/* KV entry */) {
	// Modify database value
}

func readDatabase(/* Key */) string {
	// Either simply accessing in-memory storage in KVServer, or read from disk
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