package main

import (
	"os"
	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	// Config file reading
	configFilePath := os.Args[1]
	if configFilePath == nil {
		configFilePath = "config/server_config.json"
	}
	var config raftkv.KVServerConfig
	err := util.ReadJSONConfig(configFilePath, &config)
	util.CheckErr(err, "Error reading server config: %v\n", err)
	// Tracing
	stracer := tracing.NewTracer()tracing.TracerConfig{
		ServerAddress: config.TracingServerAddr,
		TracerIdentity: config.TracerIdentity,
		Secret: config.Secret,
	})
	
	raft := raftkv.NewRaft() // Dummy name, depends on raft implementation
	server := raftkv.NewServer()
	server.Start(raft /*  +Config Values */)
	raft.Start(/* +Config Values */)
}