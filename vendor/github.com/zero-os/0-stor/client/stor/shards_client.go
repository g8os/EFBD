package stor

import (
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/stor/common"
)

// ShardsClient is 0-stor client which work on multiple
// shards instead of single shard
type ShardsClient struct {
	conf      Config
	clients   map[string]Client
	org       string
	namespace string
	iyoToken  string
}

func NewShardsClient(conf Config, shards []string, org, namespace, iyoToken string) (*ShardsClient, error) {
	sc := ShardsClient{
		conf:      conf,
		org:       org,
		namespace: namespace,
		clients:   make(map[string]Client),
		iyoToken:  iyoToken,
	}
	return &sc, sc.createClients(shards)
}

// create clients if not exists
func (sc *ShardsClient) createClients(shards []string) error {
	for _, shard := range shards {
		if _, ok := sc.clients[shard]; ok {
			continue
		}
		if _, err := sc.createClient(shard); err != nil {
			return err
		}
	}
	return nil
}

func (sc *ShardsClient) createClient(shard string) (Client, error) {
	conf := sc.conf
	conf.Shard = shard
	cli, err := NewClientWithToken(&conf, sc.org, sc.namespace, sc.iyoToken)
	if err != nil {
		return nil, err
	}
	sc.clients[shard] = cli
	return cli, nil
}

func (sc *ShardsClient) getClient(shard string) (Client, error) {
	cli, exists := sc.clients[shard]
	if exists {
		return cli, nil
	}
	return sc.createClient(shard)
}

// ObjectCreate creates an object for given key and value.
// It selects the server randomly and try until success or
// all servers has been tried.
func (sc *ShardsClient) ObjectCreate(key, val []byte, refList []string) (*meta.Meta, error) {
	var cli Client
	var err error
	var shard string

	for shard, cli = range sc.clients { // select the server randomly
		_, err = cli.ObjectCreate(key, val, refList)
		if err == nil {
			break
		}
	}

	return meta.New(key, uint64(len(val)), []string{shard})
}

// ObjectGet gets the data based on info in the metadata
func (sc *ShardsClient) ObjectGet(md *meta.Meta) ([]byte, error) {
	key, err := md.Key()
	if err != nil {
		return nil, err
	}

	// get server shards
	shards, err := md.GetShardsSlice()
	if err != nil {
		return nil, err
	}

	var obj *common.Object
	var cli Client

	for _, shard := range shards {
		cli, err = sc.getClient(shard)
		if err != nil {
			continue
		}

		obj, err = cli.ObjectGet(key)
		if err == nil {
			return obj.Value, nil
		}
	}
	return nil, err

}
