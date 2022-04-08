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

type PutRes struct {
	ClientId string
	OpId     uint8
	Key      string
	Value    string
	PToken   tracing.TracingToken
}

// MakeConnection Creates and returns a TCP connection between localAddr and remoteAddr
func MakeConnection(localAddr string, remoteAddr string) *net.TCPConn {
	localTcpAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	CheckErr(err, "Could not resolve address: "+localAddr)
	remoteTcpAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	CheckErr(err, "Could not resolve address: "+remoteAddr)
	conn, err := net.DialTCP("tcp", localTcpAddr, remoteTcpAddr)
	CheckErr(err, "Could not connect "+localAddr+" to "+remoteAddr)
	return conn
}

// MakeClient Creates an RPC client given between a local and remote address
func MakeClient(localAddr string, remoteAddr string) (*net.TCPConn, *rpc.Client) {
	conn := MakeConnection(localAddr, remoteAddr)
	client := rpc.NewClient(conn)
	return conn, client
}

// TryMakeConnection Creates and returns a TCP connection between localAddr and remoteAddr, returns an error if unsuccessful
func TryMakeConnection(localAddr string, remoteAddr string) (*net.TCPConn, error) {
	localTcpAddr, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil { return nil, err }
	remoteTcpAddr, err := net.ResolveTCPAddr("tcp", remoteAddr)
	if err != nil { return nil, err }
	conn, err := net.DialTCP("tcp", localTcpAddr, remoteTcpAddr)
	if err != nil { return nil, err }
	return conn, nil
}

// TryMakeClient Creates an RPC client given between a local and remote address, returns an error if unsuccessful
func TryMakeClient(localAddr string, remoteAddr string) (*net.TCPConn, *rpc.Client, error) {
	conn, err := TryMakeConnection(localAddr, remoteAddr)
	if err != nil { return nil, nil, err }
	client := rpc.NewClient(conn)
	return conn, client, nil
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
	client, err := rpc.DialHTTP("tcp", address)
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
