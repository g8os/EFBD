package decoder

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"

	"github.com/zero-os/0-Disk/tlog"
)

var (
	// ErrNilLastHash indicates that there is no last hash entry
	ErrNilLastHash = errors.New("nil last hash")
)

// GetLashHashKey returns last hash key of
// a given vdisk ID
func GetLashHashKey(vdiskID string) []byte {
	return []byte(tlog.LastHashPrefix + vdiskID)
}

// GetLastHash returns valid last hash of a vdisk.
// It checks all data shards to get latest valid hash
func (d *Decoder) GetLastHash() ([]byte, error) {
	var maxSeq uint64
	var lastHash []byte

	// get all valid hashes from all data storages
	for i := 0; i < d.k+d.m; i++ {
		hash, lastSeq, err := d.getLastHashFromShard(i)
		if err != nil {
			continue
		}
		if lastSeq > maxSeq {
			maxSeq = lastSeq
			lastHash = hash
		}
	}

	// no hash found, return Nil
	if len(lastHash) == 0 {
		return nil, ErrNilLastHash
	}

	return lastHash, nil
}

// get a valid last hash from a shard
func (d *Decoder) getLastHashFromShard(idx int) ([]byte, uint64, error) {
	rc := d.pool.DataConnection(idx)
	defer rc.Close()

	key := GetLashHashKey(d.vdiskID)

	hashes, err := redis.ByteSlices(rc.Do("LRANGE", key, 0, -1))
	if err == redis.ErrNil {
		return nil, 0, ErrNilLastHash
	}

	if err != nil {
		return nil, 0, err
	}

	if len(hashes) == 0 {
		return nil, 0, ErrNilLastHash
	}

	// check that the hash really valid
	for _, hash := range hashes {
		if seq, err := d.checkLastHash(hash); err == nil {
			return hash, seq, nil
		}
	}

	return nil, 0, fmt.Errorf("no valid hash for vdiskID: %s", d.vdiskID)
}

// make sure that a last hash value is valid
func (d *Decoder) checkLastHash(hash []byte) (uint64, error) {
	agg, err := d.get(hash)
	if err != nil {
		return 0, err
	}

	// check latest sequence
	blocks, err := agg.Blocks()
	if blocks.Len() == 0 {
		return 0, fmt.Errorf("empty block")
	}

	// last block always the last sequence
	return blocks.At(blocks.Len() - 1).Sequence(), nil
}
