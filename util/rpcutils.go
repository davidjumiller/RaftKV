package util

import (
	"fmt"
	"net"
	"net/rpc"

	"github.com/DistributedClocks/tracing"
)

type GetArgs struct {
	ClientId string
	Key      string
	OpId     uint8
	GToken   tracing.TracingToken
}

type GetRes struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string // Note: this should be "" if a Put for this key does not exist
	GToken   tracing.TracingToken
}

type PutArgs struct {
	ClientId string
	Key      string
	Value    string
	OpId     uint8
	PToken   tracing.TracingToken
}

type RaftPutReq struct {
	ClientId string
	Key      string
	Value    string
	OpId     uint8
}

type PutRes struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
	PToken   tracing.TracingToken
}

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
		fmt.Printf("%v \n", err)
		e.Client = nil
		return err
	}
	return nil
}

func Connect(address string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Printf("%v \n", err)
		return nil, err
	}

	return client, nil
}

// StartRPCListener starts accepting RPCs at given rpcListenAddr
func StartRPCListener(rpcListenAddr string) (*net.TCPListener, error) {
	resolvedRPCListenAddr, err := net.ResolveTCPAddr("tcp", rpcListenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", resolvedRPCListenAddr)
	if err != nil {
		return nil, err
	}
	go rpc.Accept(listener)
	return listener, nil
}
