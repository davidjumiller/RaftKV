package util

import (
	"log"
	"net/rpc"
)

type RPCEndPoint struct {
	Addr   string
	Client *rpc.Client
}

func (e *RPCEndPoint) Call(methodName string, args interface{}, reply interface{}) error {
	if e.Client == nil {
		client, err := Connect(e.Addr)
		if err != nil {
			return err
		}
		e.Client = client
	}

	err := e.Client.Call(methodName, args, reply)
	if err != nil {
		log.Println(err)
		e.Client = nil
		return err
	}
	return nil
}

func Connect(address string) (*rpc.Client, error) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return client, nil
}
