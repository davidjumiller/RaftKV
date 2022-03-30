package main

import (
	"cs.ubc.ca/cpsc416/p1/kvslib"
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

	client := kvslib.NewKVS()
	notifCh, err := client.Start(tracer, config.ClientID, config.LocalServerIPPort, config.ServerIPPortList, config.ChCapacity)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	// Put a key-value pair
	err = client.Put(tracer, "clientID1", "key2", "value2")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err)

	// Get a key's value
	err = client.Get(tracer, "clientID1", "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err)

	for i := 0; i < 2; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
}
