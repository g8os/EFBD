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

func (s *shard) Set(hashIndex int64, hash Hash) {
	offset := hashIndex * HashSize

	if hash == nil {
		hash = NilHash
	}

	copy(s.hashes[offset:offset+HashSize], hash)
	s.dirty = true
	return
}

func (s *shard) Get(hashIndex int64) (hash Hash) {
	hash = NewHash()
	offset := hashIndex * HashSize
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
