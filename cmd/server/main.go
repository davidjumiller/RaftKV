package main

import (
	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	// TODO: Config file reading
	// TODO: Tracing
	raft := raftkv.NewRaft() // Dummy name, depends on raft implementation
	server := raftkv.NewServer()
	server.Start(raft /*  +Config Values */)
	raft.Start(/* +Config Values */)
}