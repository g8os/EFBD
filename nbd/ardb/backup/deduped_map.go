package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"

	"github.com/zeebo/bencode"
)

// NewDedupedMap creates a new deduped map,
// which contains all the metadata stored for a(n) (exported) backup.
// See `DedupedMap` for more information.
func NewDedupedMap() *DedupedMap {
	return &DedupedMap{
		hashes: make(map[int64]zerodisk.Hash),
	}
}

// LoadDedupedMap a deduped map from a given (backup) server.
func LoadDedupedMap(id string, src StorageDriver, key *CryptoKey, ct CompressionType) (*DedupedMap, error) {
	buf := bytes.NewBuffer(nil)
	err := src.GetDedupedMap(id, buf)
	if err != nil {
		// deduped map did exist,
		// but an unknown error was triggered while fetching it
		return nil, err
	}

	// try to load the existing deduped map in memory
	return DeserializeDedupedMap(key, ct, buf)
}

// ExistingOrNewDedupedMap tries to first fetch an existing deduped map from a given server,
// if it doesn't exist yet, a new one will be created in-memory instead.
// If it did exist already, it will be decrypted, decompressed and loaded in-memory as a DedupedMap.
// When `force` is `true`, a new map will be created, even if one existed already but couldn't be loaded.
// When `force` is `false`, and a map exists but can't be a loaded,
// the error of why it couldn't be loaded, is returned instead.
func ExistingOrNewDedupedMap(id string, src StorageDriver, key *CryptoKey, ct CompressionType, force bool) (*DedupedMap, error) {
	buf := bytes.NewBuffer(nil)
	err := src.GetDedupedMap(id, buf)

	if err == ErrDataDidNotExist {
		// deduped map did not exist yet, return a new one
		return NewDedupedMap(), nil
	}
	if err != nil {
		// deduped map did exist, but we couldn't load it.
		if force {
			// we forcefully create a new one anyhow if `force == true`
			log.Debugf(
				"couldn't read deduped map '%s' due to an error (%s), forcefully creating a new one",
				id, err)
			return NewDedupedMap(), nil
		}
		// deduped map did exist,
		// but an unknown error was triggered while fetching it
		return nil, err
	}

	// try to load the existing deduped map in memory
	dm, err := DeserializeDedupedMap(key, ct, buf)
	if err != nil {
		// deduped map did exist, but we couldn't deserialize it.
		// This could for example happen in case the given encryption key is false,
		// or the compression algorithm doesn't match with the one used during serialization.

		// However, when `force` is given, we'll ignore this err and return a new deduped map instead.
		if force {
			log.Debugf(
				"couldn't deserialize deduped map '%s' due to an error (%s), forcefully creating a new one",
				id, err)
			return NewDedupedMap(), nil
		}

		return nil, err
	}

	log.Debugf("loaded and deserialized existing deduped map %s", id)
	return dm, err
}

// DeserializeDedupedMap allows you to deserialize a deduped map from a given reader.
// It is expected that all the data in the reader is available,
// and is compressed and (only than) encrypted.
// This function will attempt to decrypt and decompress the read data,
// using the given private (AES) key and compression type.
// The given compression type and private key has to match the information,
// used to serialize this DedupedMap in the first place.
// See `DedupedMap` for more information.
func DeserializeDedupedMap(key *CryptoKey, ct CompressionType, src io.Reader) (*DedupedMap, error) {
	decompressor, err := NewDecompressor(ct)
	if err != nil {
		return nil, err
	}

	var bufB *bytes.Buffer

	if key.Defined() {
		bufA := bytes.NewBuffer(nil)

		err = Decrypt(key, src, bufA)
		if err != nil {
			return nil, fmt.Errorf("couldn't decrypt compressed deduped map: %v", err)
		}

		bufB = bytes.NewBuffer(nil)
		err = decompressor.Decompress(bufA, bufB)
		if err != nil {
			return nil, fmt.Errorf("couldn't decompress deduped map: %v", err)
		}
	} else {
		bufB = bytes.NewBuffer(nil)

		err = decompressor.Decompress(src, bufB)
		if err != nil {
			return nil, fmt.Errorf("couldn't decompress deduped map: %v", err)
		}
	}

	hashes, err := deserializeHashes(bufB)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode bencoded deduped map: %v", err)
	}

	return &DedupedMap{hashes: hashes}, nil
}

// DedupedMap contains all hashes for a vdisk's backup,
// where each hash is mapped to its (export) block index.
// NOTE: DedupedMap is not thread-safe,
//       and should only be used on one goroutine at a time.
type DedupedMap struct {
	hashes map[int64]zerodisk.Hash
	mux    sync.Mutex
}

// SetHash sets the given hash, mapped to the given (export block) index.
// If there is already a hash mapped to the given (export block) index,
// and the hash equals the given hash, the given hash won't be used and `false` wil be returned.
// Otherwise the given hash is mapped to the given index and `true`` will be returned.
func (dm *DedupedMap) SetHash(index int64, hash zerodisk.Hash) bool {
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
func (dm *DedupedMap) GetHash(index int64) (zerodisk.Hash, bool) {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	hash, found := dm.hashes[index]
	return hash, found
}

// Serialize allows you to write all data of this map in a binary encoded manner,
// to the given writer. The encoded data will be compressed and encrypted before being
// writen to the given writer.
// You can re-load this map in memory using the `DeserializeDedupedMap` function.
func (dm *DedupedMap) Serialize(key *CryptoKey, ct CompressionType, dst io.Writer) error {
	dm.mux.Lock()
	defer dm.mux.Unlock()

	compressor, err := NewCompressor(ct)
	if err != nil {
		return err
	}

	hmbuffer := bytes.NewBuffer(nil)
	err = serializeHashes(dm.hashes, hmbuffer)
	if err != nil {
		return fmt.Errorf("couldn't bencode dedupd map: %v", err)
	}

	if key.Defined() {
		// compress and encrypt
		imbuffer := bytes.NewBuffer(nil)
		err = compressor.Compress(hmbuffer, imbuffer)
		if err != nil {
			return fmt.Errorf("couldn't compress bencoded dedupd map: %v", err)
		}

		err = Encrypt(key, imbuffer, dst)
		if err != nil {
			return fmt.Errorf("couldn't encrypt compressed dedupd map: %v", err)
		}
	} else {
		// only compress
		err = compressor.Compress(hmbuffer, dst)
		if err != nil {
			return fmt.Errorf("couldn't compress bencoded dedupd map: %v", err)
		}
	}

	return nil
}

// serializeHashes encapsulates the entire encoding logic
// for the deduped map serialization.
func serializeHashes(hashes map[int64]zerodisk.Hash, w io.Writer) error {
	hashCount := len(hashes)
	if hashCount == 0 {
		return errors.New("deduped map is empty")
	}

	var format dedupedMapEncodeFormat
	format.Count = int64(hashCount)

	format.Indices = make([]int64, hashCount)
	format.Hashes = make([][]byte, hashCount)

	var i int
	for index, hash := range hashes {
		format.Indices[i] = index
		format.Hashes[i] = hash.Bytes()
		i++
	}

	return bencode.NewEncoder(w).Encode(format)
}

// deserializeHashes encapsulates the entire decoding logic
// for the deduped map serialization.
func deserializeHashes(r io.Reader) (map[int64]zerodisk.Hash, error) {
	var format dedupedMapEncodeFormat
	err := bencode.NewDecoder(r).Decode(&format)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode bencoded deduped map: %v", err)
	}

	if format.Count == 0 {
		return nil, errors.New("invalid count for decoded deduped map")
	}
	if format.Count != int64(len(format.Indices)) {
		return nil, errors.New("invalid index count for decoded deduped map")
	}
	if format.Count != int64(len(format.Hashes)) {
		return nil, errors.New("invalid hash count for decoded deduped map")
	}

	hashes := make(map[int64]zerodisk.Hash, format.Count)
	for i := int64(0); i < format.Count; i++ {
		hashes[format.Indices[i]] = zerodisk.Hash(format.Hashes[i])
	}

	return hashes, nil
}

// dedupedMapEncodeFormat defines the structure used to
// encode a deduped map to a binary format.
// See https://github.com/zeebo/bencode for more information.
type dedupedMapEncodeFormat struct {
	Count   int64    `bencode:"c"`
	Indices []int64  `bencode:"i"`
	Hashes  [][]byte `bencode:"h"`
}
