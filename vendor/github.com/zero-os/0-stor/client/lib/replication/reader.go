package replication

import (
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/stor"
	"github.com/zero-os/0-stor/client/stor/common"
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
	iyoClientID, iyoSecret, proto string) (*StorReader, error) {

	// create meta client
	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	// get token
	iyoClient := itsyouonline.NewClient(org, iyoClientID, iyoSecret)
	iyoToken, err := iyoClient.CreateJWT(namespace, itsyouonline.Permission{
		Read: true,
	})
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
	md, err := sr.metaCli.Get(string(metaKey))
	if err != nil {
		return nil, err
	}

	shards, err := md.GetShardsSlice()
	if err != nil {
		return nil, err
	}

	objKey, err := md.Key()
	if err != nil {
		return nil, err
	}

	var sc stor.Client
	var obj *common.Object
	for _, shard := range shards {
		sc, err = sr.getClient(shard)
		if err != nil {
			continue
		}
		obj, err = sc.ObjectGet(objKey)
		if err != nil {
			continue
		}
		return obj.Value, nil
	}
	return nil, err
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
