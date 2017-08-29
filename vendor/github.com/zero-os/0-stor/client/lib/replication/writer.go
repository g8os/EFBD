package replication

import (
	"sync"

	"github.com/zero-os/0-stor/client/lib"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/stor"
)

// StorWriter is replication which write to 0-stor.
// It is implemented as wrapper around the replication Writer
type StorWriter struct {
	w       block.Writer
	conf    Config
	clients map[string]stor.Client
	metaCli *meta.Client

	// number of replication we want to create
	// user might don't want to replicate to
	// all other servers
	numReplication int
}

func NewStorWriter(w block.Writer, conf Config, shards, metaShards []string, org, namespace,
	iyoToken, proto string) (*StorWriter, error) {
	clients := make(map[string]stor.Client)

	// create meta client
	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	// create writers for each shard
	for _, shard := range shards {
		storConf := stor.Config{
			Protocol: proto,
			Shard:    shard,
		}
		cli, err := stor.NewClientWithToken(&storConf, org, namespace, iyoToken)
		if err != nil {
			return nil, err
		}
		clients[shard] = cli
	}

	return &StorWriter{
		w:              w,
		conf:           conf,
		clients:        clients,
		metaCli:        metaCli,
		numReplication: conf.NumReplication(len(shards)),
	}, nil
}

// WriteBlock implements block.Writer.WriteBlock interface.
// If number of failed shards is more than max failed allowed,
// then the error contains the failed shards
func (sw *StorWriter) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	var shardErr lib.ShardError
	var okShards []string // list of OK shards

	// get random shards
	// to handle cases where we don't want to replicate
	// to all available servers
	shards := sw.getRandomShards()

	// write concurrently to sw.numReplication of servers
	var (
		wg  sync.WaitGroup
		mux sync.Mutex
	)

	wg.Add(sw.numReplication)
	for i := 0; i < sw.numReplication; i++ {
		go func(shard string) {
			defer wg.Done()
			cli := sw.clients[shard]
			_, err := cli.ObjectCreate(key, val, nil)
			if err != nil {
				shardErr.Add([]string{shard}, lib.ShardType0Stor, err, lib.StatusUnknownError)
				return
			}
			mux.Lock()
			defer mux.Unlock()
			okShards = append(okShards, shard)
		}(shards[i])
	}

	// wait all replicater to be finished
	wg.Wait()

	// in case of there were error
	// we write to other server sequentially
	if len(okShards) < sw.numReplication {
		for i := sw.numReplication; i < len(sw.clients); i++ {
			shard := shards[i]

			cli := sw.clients[shard]

			if _, err := cli.ObjectCreate(key, val, nil); err != nil {
				shardErr.Add([]string{shard}, lib.ShardType0Stor, err, lib.StatusUnknownError)
				continue
			}

			okShards = append(okShards, shard)
			if len(okShards) >= sw.numReplication {
				break
			}
		}
		if len(okShards) < sw.numReplication {
			return md, shardErr
		}
	}

	// update meta
	md.SetKey(key)
	md.SetShardSlice(okShards)
	md.SetSize(uint64(len(val)))

	if err := sw.metaCli.Put(string(key), md); err != nil {
		shardErr.Add(sw.metaCli.Endpoints(), lib.ShardTypeEtcd, err, lib.StatusUnknownError)
		return md, shardErr
	}

	return sw.w.WriteBlock(key, val, md)
}

func (sw *StorWriter) getRandomShards() []string {
	shards := make([]string, 0, len(sw.clients))
	for shard := range sw.clients { // map iteration is random
		shards = append(shards, shard)
	}
	return shards
}
