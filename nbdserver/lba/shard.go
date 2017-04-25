package lba

import (
	"errors"
	"fmt"
	"io"

	"github.com/g8os/blockstor"
)

const (
	//NumberOfRecordsPerLBAShard is the fixed length of the LBAShards
	NumberOfRecordsPerLBAShard = 128
	// BytesPerShard defines how many bytes each shards requires
	BytesPerShard = NumberOfRecordsPerLBAShard * blockstor.HashSize
)

var (
	errNilShardWrite = errors.New("shard is nil, and cannot be written")
)

func newShard() *shard {
	shard := new(shard)
	shard.hashes = make([]byte, BytesPerShard)
	return shard
}

func shardFromBytes(bytes []byte) (s *shard, err error) {
	if length := len(bytes); length != BytesPerShard {
		err = fmt.Errorf(
			"raw shard contains %d bytes, while expected %d bytes",
			length, BytesPerShard)
		return
	}

	s = new(shard)
	s.hashes = bytes
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

func (s *shard) Set(hashIndex int64, hash blockstor.Hash) {
	offset := hashIndex * blockstor.HashSize

	if hash == nil {
		hash = blockstor.NilHash
	}

	copy(s.hashes[offset:offset+blockstor.HashSize], hash)
	s.dirty = true
	return
}

func (s *shard) Get(hashIndex int64) (hash blockstor.Hash) {
	hash = blockstor.NewHash()
	offset := hashIndex * blockstor.HashSize
	copy(hash[:], s.hashes[offset:])
	return
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
