package meta

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	metaOpTimeout = 10 * time.Second
)

var (
	// ErrMetadataNotFound is returned when a key is not found in etcd cluster
	ErrMetadataNotFound = fmt.Errorf("key not found in etcd")
)

// Client defines client to store metadata
type Client struct {
	etcdClient *clientv3.Client
}

// NewClient creates new meta client
func NewClient(shards []string) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   shards,
		DialTimeout: metaOpTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Client{
		etcdClient: cli,
	}, nil
}

// Close closes meta client, release all resources
func (c *Client) Close() error {
	return c.etcdClient.Close()
}

// Put stores meta to metadata server
func (c *Client) Put(key string, meta *Meta) error {
	buf := new(bytes.Buffer)
	if err := meta.Encode(buf); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), metaOpTimeout)
	defer cancel()

	_, err := c.etcdClient.Put(ctx, key, string(buf.Bytes()))
	return err
}

// Get fetch metadata from metadata server
func (c *Client) Get(key string) (*Meta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaOpTimeout)
	defer cancel()

	resp, err := c.etcdClient.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) < 1 {
		return nil, ErrMetadataNotFound
	}

	return Decode(resp.Kvs[0].Value)
}

func (c *Client) Endpoints() []string {
	return c.etcdClient.Endpoints()
}
