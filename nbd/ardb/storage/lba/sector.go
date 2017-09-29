package lba

import (
	"fmt"

	"github.com/zero-os/0-Disk"
)

const (
	// NumberOfRecordsPerLBASector is the fixed length of the LBASectors
	NumberOfRecordsPerLBASector = 128
	// BytesPerSector defines how many bytes each sector requires
	BytesPerSector = NumberOfRecordsPerLBASector * zerodisk.HashSize
)

// NewSector cleans a new fresh nil-sector.
func NewSector() *Sector {
	sector := new(Sector)
	sector.hashes = make([]byte, BytesPerSector)
	return sector
}

// SectorFromBytes allows you to create a sector directly from a given byte slice.
// Note that the given byte slice's length hash to be equal to `BytesPerSector`.
func SectorFromBytes(bytes []byte) (s *Sector, err error) {
	if length := len(bytes); length != BytesPerSector {
		err = fmt.Errorf(
			"raw sector contains %d bytes, while expected %d bytes",
			length, BytesPerSector)
		return
	}

	s = new(Sector)
	s.hashes = bytes
	return
}

// Sector defines a group of hashes,
// stored together as a single value,
// as part of the bigger LBA map of a deduped vdisk.
type Sector struct {
	hashes []byte
	dirty  bool
}

// Dirty returns if a sector has been modified since it has last been modified.
// It can be marked clean again using the UnsetDirty function.
func (s *Sector) Dirty() bool {
	return s.dirty
}

// UnsetDirty allows you to unmark a dirty sector.
func (s *Sector) UnsetDirty() {
	s.dirty = false
}

// Set a hash for a given index.
func (s *Sector) Set(hashIndex int64, hash zerodisk.Hash) {
	offset := hashIndex * zerodisk.HashSize

	if hash == nil {
		hash = zerodisk.NilHash
	}

	copy(s.hashes[offset:offset+zerodisk.HashSize], hash)
	s.dirty = true
	return
}

// Get a hash using a given index.
// Returning nil if this hash could not be found.
func (s *Sector) Get(hashIndex int64) zerodisk.Hash {
	offset := hashIndex * zerodisk.HashSize
	targetHash := zerodisk.Hash(s.hashes[offset : offset+zerodisk.HashSize])
	if targetHash.Equals(zerodisk.NilHash) {
		return nil
	}

	hash := zerodisk.NewHash()
	copy(hash[:], targetHash)
	return hash
}

// IsNil returns false if this sector contains no non-nil hash.
func (s *Sector) IsNil() bool {
	for _, b := range s.hashes {
		if b != 0 {
			return false
		}
	}

	return true
}

// Bytes returns this sector's internal byte slice,
// NOTE: this should never be used for non-storage purposes.
func (s *Sector) Bytes() []byte {
	if s.IsNil() {
		return nil
	}

	return s.hashes
}
