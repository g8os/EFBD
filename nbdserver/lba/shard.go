package lba

import (
	"errors"
	"fmt"
	"io"
)

const (
	//NumberOfRecordsPerLBAShard is the fixed length of the LBAShards
	NumberOfRecordsPerLBAShard = 128
	// BytesPerShard defines how many bytes each shards requires
	BytesPerShard = NumberOfRecordsPerLBAShard * HashSize
)

var (
	errNilShardWrite = errors.New("shard is nil, and cannot be written")
)

func newShard() *shard {
	shard := new(shard)
	shard.hashes = make([]byte, BytesPerShard)
	return shard
}

func shardFromBytes(bytes []byte) (shard *shard, err error) {
	if len(bytes) < BytesPerShard {
		err = fmt.Errorf("raw shard is too small, expected %d bytes", BytesPerShard)
		return
	}

	shard = newShard()
	shard.hashes = bytes
	return
}

type shard struct {
	hashes []byte
	dirty  bool
}

func (s *shard) Dirty() bool {
	return s.dirty
}

func (s *shard) UnsetDirty() {
	s.dirty = false
}

func (s *shard) Set(hashIndex int64, hash Hash) {
	offset := hashIndex * HashSize
	if hash == nil {
		hash = nilHash
	}

	copy(s.hashes[offset:offset+HashSize], hash)
	s.dirty = true
}

func (s *shard) Get(hashIndex int64) Hash {
	offset := hashIndex * HashSize
	return Hash(s.hashes[offset : offset+HashSize])
}

func (s *shard) IsNil() bool {
	for _, b := range s.hashes {
		if b != 0 {
			return false
		}
	}

	return true
}

func (s *shard) Write(w io.Writer) (err error) {
	if s.IsNil() {
		err = errNilShardWrite
		return
	}

	_, err = w.Write(s.hashes)
	return
}
