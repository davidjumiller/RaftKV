package main

import (
	"fmt"
	"os"
	"strconv"

	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	// Get server index from command line arg
	serverIdx, err := strconv.Atoi(os.Args[1])
	util.CheckErr(err, "failed to parse server index")

	// Read server config
	// filename := fmt.Sprintf("./config/server_config_%d.json", serverIdx)
	filename := "./config/server_config.json"
	fmt.Println("Server config file:", filename)
	var config raftkv.KVServerConfig
	err = util.ReadJSONConfig(filename, &config)
	util.CheckErr(err, "failed to locate or parse config for server %d", serverIdx)

	// Create tracers for Server and Raft
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: fmt.Sprintf("server%d", serverIdx),
		Secret:         config.Secret,
	})
	rtracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: fmt.Sprintf("raft%d", serverIdx),
		Secret:         config.Secret,
	})

	// Start Raft
	var peers []*util.RPCEndPoint
	for _, raftAddr := range config.RaftList {
		peer := &util.RPCEndPoint{raftAddr, nil}
		peers = append(peers, peer)
	}
	persister := util.MakePersister()
	applyCh := make(chan raftkv.ApplyMsg)
	raft, err := raftkv.StartRaft(peers, serverIdx, persister, applyCh, rtracer)

	// Start Server
	server := raftkv.NewServer()
	err = server.Start(serverIdx, config.ServerList, stracer, raft)
}
