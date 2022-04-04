package main

import (
	"cs.ubc.ca/cpsc416/p1/raftkv"
	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
	"log"
)

type ClientConfig struct {
	ClientID          string
	LocalServerIPPort string
	ServerIPPortList  []string
	ChCapacity        int
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

func main() {
	var config ClientConfig
	err := util.ReadJSONConfig("config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	client := raftkv.NewKVS()
	notifCh, err := client.Start(tracer, config.ClientID, config.LocalServerIPPort, config.ServerIPPortList, config.ChCapacity)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	// Put a key-value pair
	err = client.Put(tracer, "key2", "value2")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err)

	// Get a key's value
	err = client.Get(tracer, "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err)

	// Sequence of interleaved gets and puts
	err = client.Put(tracer, "key1", "test1")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err)
	err = client.Get(tracer, "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err)
	err = client.Put(tracer, "key1", "test2")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err)
	err = client.Get(tracer, "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err)
	err = client.Get(tracer, "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err)
	err = client.Put(tracer, "key1", "test3")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err)
	err = client.Get(tracer, "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err)

	for i := 0; i < 9; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
}
