package replication

import (
	"fmt"
	"strings"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/stor"
)

// StorWriter is replication which write to 0-stor.
// It is implemented as wrapper around the replication Writer
type StorWriter struct {
	w         block.Writer
	conf      Config
	maxFailed int
	writers   map[string]block.Writer
	metaCli   *meta.Client
}

func NewStorWriter(w block.Writer, conf Config, shards, metaShards []string, org, namespace,
	iyoClientID, iyoSecret, proto string) (*StorWriter, error) {
	writers := make(map[string]block.Writer)

	// create meta client
	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	// create writers for each shard
	for _, shard := range shards {
		storConf := stor.Config{
			Protocol:    proto,
			Shard:       shard,
			IyoClientID: iyoClientID,
			IyoSecret:   iyoSecret,
		}
		ssw, err := stor.NewSingleShardWriter(storConf, org, namespace)
		if err != nil {
			return nil, err
		}
		writers[shard] = ssw
	}

	return &StorWriter{
		w:         w,
		conf:      conf,
		writers:   writers,
		maxFailed: len(writers) - conf.Number,
		metaCli:   metaCli,
	}, nil
}

// WriteBlock implements block.Writer.WriteBlock interface.
// If number of failed shards is more than max failed allowed,
// then the error contains the failed shards
func (sw *StorWriter) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	var err error
	var failShards []string

	// create the replicater from existing writer
	writer := NewWriter(sw.getRandomWriters(), sw.conf)

	// replicate
	failedWriters, md, err := writer.Write(key, val, md)

	// return error if we can't replicate to conf.Number 0-stor server
	if err != nil || len(failedWriters) > sw.maxFailed {
		for _, fw := range failedWriters {
			ssw := fw.(*stor.SingleShardWriter)
			failShards = append(failShards, ssw.Shard)
		}
		return md, fmt.Errorf(strings.Join(failShards, ","))
	}

	// update meta
	md.SetKey(key)
	md.SetShardSlice(sw.getOKShards(failShards))
	md.SetSize(uint64(len(val)))
	md.SetEpochNow()

	if err := sw.metaCli.Put(string(key), md); err != nil {
		return md, err
	}

	return sw.w.WriteBlock(key, val, md)
}

func (sw *StorWriter) getRandomWriters() []block.Writer {
	writers := make([]block.Writer, 0, len(sw.writers))
	for _, w := range sw.writers { // map iteration is random
		writers = append(writers, w)
	}
	return writers
}

// return the shards that can successfully uploaded
func (sw *StorWriter) getOKShards(failShards []string) []string {
	// initialize the ok shards map
	okShards := make(map[string]struct{}, len(sw.writers)-len(failShards))
	for shard, _ := range sw.writers {
		okShards[shard] = struct{}{}
	}

	// delete failed shards from the ok shards
	for _, shard := range failShards {
		delete(okShards, shard)
	}

	// create the ok shards slice
	okShardsSlice := make([]string, 0, len(okShards))
	for shard, _ := range okShards {
		okShardsSlice = append(okShardsSlice, shard)
	}
	return okShardsSlice
}
