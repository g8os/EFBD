package stor

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	metaOpTimeout = 10 * time.Second
)

func newEtcdClient(shards []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   shards,
		DialTimeout: metaOpTimeout,
	})
}

func (c *Client) getRawMeta(key []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaOpTimeout)
	defer cancel()

	resp, err := c.metaCli.Get(ctx, string(key))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, nil
	}

	return resp.Kvs[0].Value, nil

}

func (c *Client) getFirstMetaKey() ([]byte, error) {
	return c.getRawMeta(c.firstMetaEtcdKey)
}

func (c *Client) saveFirstMetaKey() error {
	return c.saveRawMeta(c.firstMetaEtcdKey, c.firstMetaKey)
}

func (c *Client) getLastMetaKey() ([]byte, error) {
	return c.getRawMeta(c.lastMetaEtcdKey)
}

func (c *Client) saveLastMetaKey() error {
	return c.saveRawMeta(c.lastMetaEtcdKey, c.lastMetaKey)
}

func (c *Client) saveRawMeta(key, val []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), metaOpTimeout)
	defer cancel()

	_, err := c.metaCli.Put(ctx, string(key), string(val))
	return err
}
