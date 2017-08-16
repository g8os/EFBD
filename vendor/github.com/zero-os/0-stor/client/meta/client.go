package meta

import (
	"bytes"
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
)

// Client defines client to store metadata
type Client struct {
	etcdClient *clientv3.Client
}

// NewClient creates new meta client
func NewClient(shards []string) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: shards,
	})
	if err != nil {
		return nil, err
	}
	return &Client{
		etcdClient: cli,
	}, nil
}

// Close closes meta client, release all resources
func (c *Client) Close() {
	c.etcdClient.Close()
}

// Put stores meta to metadata server
func (c *Client) Put(key string, meta *Meta) error {
	buf := new(bytes.Buffer)
	if err := meta.Encode(buf); err != nil {
		return err
	}
	_, err := c.etcdClient.Put(context.TODO(), key, string(buf.Bytes()))
	return err
}

// Get fetch metadata from metadata server
func (c *Client) Get(key string) (*Meta, error) {
	resp, err := c.etcdClient.Get(context.TODO(), key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, fmt.Errorf("invalid number of kvs returned: %v", len(resp.Kvs))
	}

	return Decode(resp.Kvs[0].Value)
}
