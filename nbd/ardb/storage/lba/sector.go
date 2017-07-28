package lba

import (
	"errors"
	"fmt"
	"io"

	"github.com/zero-os/0-Disk"
)

const (
	// NumberOfRecordsPerLBASector is the fixed length of the LBASectors
	NumberOfRecordsPerLBASector = 128
	// BytesPerSector defines how many bytes each sector requires
	BytesPerSector = NumberOfRecordsPerLBASector * zerodisk.HashSize
)

var (
	// internal error which is returned
	// in case a pure nil sector is being written
	errNilSectorWrite = errors.New("sector is nil, and cannot be written")
)

func newSector() *sector {
	sector := new(sector)
	sector.hashes = make([]byte, BytesPerSector)
	return sector
}

func sectorFromBytes(bytes []byte) (s *sector, err error) {
	if length := len(bytes); length != BytesPerSector {
		err = fmt.Errorf(
			"raw sector contains %d bytes, while expected %d bytes",
			length, BytesPerSector)
		return
	}

	s = new(sector)
	s.hashes = bytes
	return
}

type sector struct {
	hashes []byte
	dirty  bool
}

func (s *sector) Dirty() bool {
	return s.dirty
}

func (s *sector) UnsetDirty() {
	s.dirty = false
}

func (s *sector) Set(hashIndex int64, hash zerodisk.Hash) {
	offset := hashIndex * zerodisk.HashSize

	if hash == nil {
		hash = zerodisk.NilHash
	}

	copy(s.hashes[offset:offset+zerodisk.HashSize], hash)
	s.dirty = true
	return
}

func (s *sector) Get(hashIndex int64) (hash zerodisk.Hash) {
	hash = zerodisk.NewHash()
	offset := hashIndex * zerodisk.HashSize
	copy(hash[:], s.hashes[offset:])
	return
}

func (s *sector) IsNil() bool {
	for _, b := range s.hashes {
		if b != 0 {
			return false
		}
	}

	return true
}

func (s *sector) Write(w io.Writer) (err error) {
	if s.IsNil() {
		err = errNilSectorWrite
		return
	}

	_, err = w.Write(s.hashes)
	return
}
