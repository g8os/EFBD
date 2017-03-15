package lba

import (
	"fmt"
	"io"
)

const (
	//NumberOfRecordsPerLBAShard is the fixed length of the LBAShards
	NumberOfRecordsPerLBAShard = 128
	// BytesPerShard defines how many bytes each shards requires
	BytesPerShard = NumberOfRecordsPerLBAShard * HashSize
)

func newShard() *shard {
	return new(shard)
}

func shardFromBytes(bytes []byte) (shard *shard, err error) {
	if len(bytes) < BytesPerShard {
		err = fmt.Errorf("raw shard is too small, expected %d bytes", BytesPerShard)
		return
	}

	shard = newShard()
	for i := 0; i < NumberOfRecordsPerLBAShard; i++ {
		var h Hash
		copy(h[:], bytes[i*HashSize:])
		shard.hashes[i] = &h
	}

	return
}

type shard struct {
	hashes [NumberOfRecordsPerLBAShard]*Hash
	dirty  bool
}

func (s *shard) Dirty() bool {
	return s.dirty
}

func (s *shard) Set(hashIndex int64, hash *Hash) {
	s.hashes[hashIndex] = hash
	s.dirty = true
}

func (s *shard) Get(hashIndex int64) *Hash {
	return s.hashes[hashIndex]
}

func (s *shard) Write(w io.Writer) (err error) {
	var h *Hash
	for _, h = range s.hashes {
		if h != nil {
			if _, err = w.Write((*h)[:]); err != nil {
				return
			}
		} else {
			if _, err = w.Write(nilHash[:]); err != nil {
				return
			}
		}
	}

	return
}
