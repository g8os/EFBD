package replication

import (
	"github.com/zero-os/0-stor/client/lib"
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/stor"
)

type StorReader struct {
	conf      Config
	clients   map[string]stor.Client
	metaCli   *meta.Client
	protocol  string
	iyoToken  string
	org       string
	namespace string
}

func NewStorReader(conf Config, shards, metaShards []string, org, namespace,
	iyoToken, proto string) (*StorReader, error) {

	// create meta client
	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	return &StorReader{
		conf:      conf,
		clients:   make(map[string]stor.Client),
		metaCli:   metaCli,
		protocol:  proto,
		org:       org,
		namespace: namespace,
		iyoToken:  iyoToken,
	}, nil
}

func (sr *StorReader) ReadBlock(metaKey []byte) ([]byte, error) {
	var shardErr lib.ShardError

	// get metadata
	md, err := sr.metaCli.Get(string(metaKey))
	if err != nil {
		shardErr.Add(sr.metaCli.Endpoints(), lib.ShardType0Stor, err, lib.StatusUnknownError)
		return nil, shardErr
	}

	// get shards from the metadata
	shards, err := md.GetShardsSlice()
	if err != nil {
		return nil, err
	}

	// get object key
	objKey, err := md.Key()
	if err != nil {
		return nil, err
	}

	for _, shard := range shards {
		sc, err := sr.getClient(shard)
		if err != nil {
			shardErr.Add([]string{shard}, lib.ShardType0Stor, err, lib.StatusInvalidShardAddress)
			continue
		}

		obj, err := sc.ObjectGet(objKey)
		if err != nil {
			shardErr.Add([]string{shard}, lib.ShardType0Stor, err, lib.StatusUnknownError)
			continue
		}

		return obj.Value, nil
	}
	return nil, shardErr
}

func (sr *StorReader) getClient(shard string) (stor.Client, error) {
	if sc, ok := sr.clients[shard]; ok {
		return sc, nil
	}
	storConf := stor.Config{
		Protocol: sr.protocol,
		Shard:    shard,
	}
	sc, err := stor.NewClientWithToken(&storConf, sr.org, sr.namespace, sr.iyoToken)
	if err != nil {
		return nil, err
	}

	sr.clients[shard] = sc
	return sc, nil
}
