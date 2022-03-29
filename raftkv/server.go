package raftkv

import (

)

type GetArgs struct {

}

type GetRes struct {

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
	isLeader	bool
	serverList	[]string
	log 		[]LogEntry
	numServers	uint8
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

func (kvs *KVServer) Get(getArgs *GetArgs, getRes *GetRes) error {

	if kvs.isLeader {
		// Access personal storage, retrieve value
	} else {
		// Send Get request to Leader
		// Receive value and send to client
	}

	return nil
}

func (kvs *KVServer) Put(putArgs *PutArgs, putRes *PutRes) error {

	if kvs.isLeader {
		// Modify personal storage
		// Modify log
		// Disseminate log to other servers
	} else {
		// Send Put request to leader
	}

	return nil
}