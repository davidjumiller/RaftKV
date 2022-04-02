package main

import (
	"os"
	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"strconv"
)

func main() {
	// Get server ID from command line arg
	serverId, err := strconv.Atoi(os.Args[1])
	util.CheckErr(err, "failed to parse server ID")

	// Read server config
	filename := fmt.Sprintf("./config/server_config_%v.json", serverId)
	fmt.Print(filename)
	var config raftkv.KVServerConfig
	err = util.ReadJSONConfig(filename, &config)
	util.CheckErr(err, "failed to locate or parse config for server %d", serverId)

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
	raft := raftkv.Start(peers, serverId, persister, applyCh)

	// Start Server
	server := raftkv.NewServer()
	err = server.Start(serverId, config.ServerAddr, config.ServerListenAddr, config.ServerList, stracer, raft)
}
