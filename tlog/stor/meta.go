package stor

import (
	"context"

	"github.com/coreos/etcd/clientv3"
)

func newEtcdClient(shards []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints: shards,
	})
}

func (c *Client) getRawMeta(key []byte) ([]byte, error) {
	resp, err := c.metaCli.Get(context.TODO(), string(key))
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
	_, err := c.metaCli.Put(context.TODO(), string(key), string(val))
	return err
}
