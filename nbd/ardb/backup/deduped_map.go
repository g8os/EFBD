package backup

import (
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/errors"
)

// unpackRawDedupedMap allows you to unpack a raw deduped map
// and start using it as an actual dedupedMap.
// If the count of the given raw deduped map is `0`, a new dedupedMap is created instead.
// NOTE: the slice (hashes) data will be shared amongst the raw and real deduped map,
// so ensure that this is OK.
func unpackRawDedupedMap(raw RawDedupedMap) (*dedupedMap, error) {
	if raw.Count == 0 {
		return newDedupedMap(), nil
	}
	err := raw.Validate()
	if err != nil {
		return nil, err
	}

	hashes := make(map[int64]zerodisk.Hash, raw.Count)
	for i := int64(0); i < raw.Count; i++ {
		hashes[raw.Indices[i]] = zerodisk.Hash(raw.Hashes[i])
	}

	return &dedupedMap{hashes: hashes}, nil
}

// newDedupedMap creates a new deduped map,
// which contains all the metadata stored for a(n) (exported) backup.
// See `dedupedMap` for more information.
func newDedupedMap() *dedupedMap {
	return &dedupedMap{
		hashes: make(map[int64]zerodisk.Hash),
	}
}

// dedupedMap contains all hashes for a vdisk's backup,
// where each hash is mapped to its (export) block index.
type dedupedMap struct {
	hashes map[int64]zerodisk.Hash
	mux    sync.Mutex
}

// SetHash sets the given hash, mapped to the given (export block) index.
// If there is already a hash mapped to the given (export block) index,
// and the hash equals the given hash, the given hash won't be used and `false` wil be returned.
// Otherwise the given hash is mapped to the given index and `true`` will be returned.
func (dm *dedupedMap) SetHash(index int64, hash zerodisk.Hash) bool {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	if h, found := dm.hashes[index]; found && h.Equals(hash) {
		return false
	}

	dm.hashes[index] = hash
	return true
}

// GetHash returns the hash which is mapped to the given (export block) index.
// `false` is returned in case no hash is mapped to the given (export block) index.
func (dm *dedupedMap) GetHash(index int64) (zerodisk.Hash, bool) {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	hash, found := dm.hashes[index]
	return hash, found
}

// Raw returns this dedupedMap as a RawDedupedMap.
// NOTE: the hash data is shared with the hashes stored in this DedupedMap,
//       so ensure that this functional is called in complete isolation
func (dm *dedupedMap) Raw() (*RawDedupedMap, error) {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	hashCount := len(dm.hashes)
	if hashCount == 0 {
		return nil, errors.New("deduped map is empty")
	}

	raw := new(RawDedupedMap)
	raw.Count = int64(hashCount)

	raw.Indices = make([]int64, hashCount)
	raw.Hashes = make([][]byte, hashCount)

	var i int
	for index, hash := range dm.hashes {
		raw.Indices[i] = index
		raw.Hashes[i] = hash.Bytes()
		i++
	}

	return raw, nil
}
