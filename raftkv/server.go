package raftkv

import (
	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"cs.ubc.ca/cpsc416/p1/kvslib"
	"github.com/DistributedClocks/tracing"
)

type GetArgs struct {

}

type GetRes struct {

}

type PutArgs struct {

}

type PutRes struct {

}

type LogEntry {

}

type ServerConfig struct {
	// Values Read from config file
}

type Server struct {
	isLeader	bool
	serverList	[]string
	log 		[]LogEntry
	numServers	uint8
}

func NewServer() *Server {
	return &Server{
		isLeader: false,
		serverList []string{},
		log: []LogEntry{},
	}
}

func (s *Server) Start() error {

}

func (s *Server) Get(getArgs *GetArgs, getRes *GetRes) error {

	if isLeader {
		// Access personal storage, retrieve value
	} else {
		// Send Get request to Leader
		// Receive value and send to client
	}


}

func (s *Server) Put(putArgs *PutArgs, putRes *PutRes) error {

	if isLeader {
		// Modify personal storage
		// Modify log
		// Disseminate log to other servers
	} else {
		// Send Put request to leader
	}

}