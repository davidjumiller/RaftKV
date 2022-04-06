package main

import (
	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"os"
	"strconv"
)

func main() {
	// Get server index from command line arg
	serverIdx, err := strconv.Atoi(os.Args[1])
	util.CheckErr(err, "failed to parse server index")

	// Read server config
	filename := fmt.Sprintf("./config/server_config_%d.json", serverIdx)
	fmt.Println("Server config file:", filename)
	var config raftkv.KVServerConfig
	err = util.ReadJSONConfig(filename, &config)
	util.CheckErr(err, "failed to locate or parse config for server %d", serverIdx)

	// Create server tracer
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	// Start Raft
	var peers []*util.RPCEndPoint
	persister := util.MakePersister()
	applyCh := make(chan raftkv.ApplyMsg)
	raft := raftkv.StartRaft(peers, serverIdx, persister, applyCh)

	// Start Server
	server := raftkv.NewServer()
	err = server.Start(serverIdx, config.ServerAddr, config.ServerList, stracer, raft)
}
