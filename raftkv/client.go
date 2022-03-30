package raftkv

import "net/rpc"

type Client struct {
	clientId string
}

type ClientEnd struct {
	Addr   string
	Client *rpc.Client
}

func MakeClient(servers []*ClientEnd) *Client {
	return nil
}

type NotifyChannel struct{}

func (c *Client) Start(key string) (NotifyChannel, error) {
	return NotifyChannel{}, nil
}
func (c *Client) Get(key string) error {
	return nil
}
func (c *Client) Put(key string, value string) error {
	return nil
}
