package util

import (
	"net"
	"net/rpc"
)

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
