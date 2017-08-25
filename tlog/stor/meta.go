package stor

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	metaOpTimeout = 10 * time.Second
)

type MetaClient struct {
	cli *clientv3.Client
}

func NewMetaClient(endpoints []string) (*MetaClient, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: metaOpTimeout,
	})

	if err != nil {
		return nil, err
	}

	return &MetaClient{
		cli: cli,
	}, nil
}

func (cli *MetaClient) Close() error {
	return cli.cli.Close()
}

func (cli *MetaClient) GetMeta(key []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), metaOpTimeout)
	defer cancel()

	resp, err := cli.cli.Get(ctx, string(key))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) != 1 {
		return nil, nil
	}

	return resp.Kvs[0].Value, nil

}

func (cli *MetaClient) SaveMeta(key, val []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), metaOpTimeout)
	defer cancel()

	_, err := cli.cli.Put(ctx, string(key), string(val))
	return err
}

func (c *Client) getFirstMetaKey() ([]byte, error) {
	return c.metaCli.GetMeta(c.firstMetaEtcdKey)
}

func (c *Client) saveFirstMetaKey() error {
	return c.metaCli.SaveMeta(c.firstMetaEtcdKey, c.firstMetaKey)
}

func (c *Client) getLastMetaKey() ([]byte, error) {
	return c.metaCli.GetMeta(c.lastMetaEtcdKey)
}

func (c *Client) saveLastMetaKey() error {
	return c.metaCli.SaveMeta(c.lastMetaEtcdKey, c.lastMetaKey)
}
