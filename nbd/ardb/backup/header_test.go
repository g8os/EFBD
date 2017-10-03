package backup

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/bencode"
	"github.com/zero-os/0-Disk"
)

func TestHeaderEncoding(t *testing.T) {
	const (
		hashCount = 4096
	)

	require := require.New(t)

	header := &Header{
		Metadata: Metadata{
			SnapshotID: "foo_backup",
			BlockSize:  4096,
			Created:    time.Now().Format(time.RFC3339),
			Source: Source{
				VdiskID:   "foo",
				BlockSize: 4096,
				Size:      16384,
			},
		},
		DedupedMap: RawDedupedMap{
			Count: hashCount,
		},
	}

	// create and set all hashes
	for i := int64(0); i < hashCount; i++ {
		hash := zerodisk.NewHash()
		_, err := rand.Read(hash[:])
		require.NoError(err)

		header.DedupedMap.Hashes = append(
			header.DedupedMap.Hashes, hash)
		header.DedupedMap.Indices = append(
			header.DedupedMap.Indices, i)
	}

	// encode
	buf := bytes.NewBuffer(nil)
	err := encodeHeader(header, buf)
	require.NoError(err)

	t.Logf(
		"header with %d hashes (%d bytes) is encoded into %d bytes",
		hashCount, ((zerodisk.HashSize + 4) * hashCount), len(buf.Bytes()))

	// decode
	outHeader, err := decodeHeader(buf)
	require.NoError(err)
	require.Equal(header.DedupedMap.Count, outHeader.DedupedMap.Count)

	for i := int64(0); i < hashCount; i++ {
		require.Equal(i, header.DedupedMap.Indices[i])
		require.Equal(i, outHeader.DedupedMap.Indices[i])
		require.Equal(header.DedupedMap.Hashes[i], outHeader.DedupedMap.Hashes[i])
	}
}

func TestStoreLoadHeader(t *testing.T) {
	const (
		hashCount  = 4096
		snapshotID = "foo_backup"
	)

	require := require.New(t)

	header := &Header{
		Metadata: Metadata{
			SnapshotID: snapshotID,
			BlockSize:  4096,
			Created:    time.Now().Format(time.RFC3339),
			Source: Source{
				VdiskID:   "foo",
				BlockSize: 4096,
				Size:      16384,
			},
		},
		DedupedMap: RawDedupedMap{
			Count: hashCount,
		},
	}

	// create and set all hashes
	var err error
	for i := int64(0); i < hashCount; i++ {
		hash := zerodisk.NewHash()
		_, err = rand.Read(hash[:])
		require.NoError(err)

		header.DedupedMap.Hashes = append(
			header.DedupedMap.Hashes, hash)
		header.DedupedMap.Indices = append(
			header.DedupedMap.Indices, i)
	}

	// store header

	driver := newStubDriver()
	err = StoreHeader(header, &privKey, LZ4Compression, driver)
	require.NoError(err)

	// load header

	outHeader, err := LoadHeader(snapshotID, driver, &privKey, LZ4Compression)
	require.NoError(err)
	require.Equal(header.DedupedMap.Count, outHeader.DedupedMap.Count)

	// verify loaded header

	for i := int64(0); i < hashCount; i++ {
		require.Equal(i, header.DedupedMap.Indices[i])
		require.Equal(i, outHeader.DedupedMap.Indices[i])
		require.Equal(header.DedupedMap.Hashes[i], outHeader.DedupedMap.Hashes[i])
	}
}

func TestLoadHeader_DeprecatedDedupedMap(t *testing.T) {
	const (
		hashCount  = 128
		snapshotID = "foo_backup"
	)

	require := require.New(t)

	header := &Header{
		Metadata: Metadata{
			SnapshotID: snapshotID,
			BlockSize:  4096,
			Created:    time.Now().Format(time.RFC3339),
			Source: Source{
				VdiskID:   "foo",
				BlockSize: 4096,
				Size:      16384,
			},
		},
	}

	// create fake snapshot blocks (and generate a deduped map that way)

	driver := newStubDriver()
	dm := NewDedupedMap()

	// NOTE: that we also need to store the deduped blocks
	pipeline := testExportPipeline(t, driver, dm)
	// as this one is read to get the block size in a slightly hacky way
	for index := int64(0); index < hashCount; index++ {
		err := pipeline.WriteBlock(index, []byte{1, 2, 3, 4, 5, 6, 7, 8})
		require.NoError(err)
	}

	rawDedupedMap, err := dm.Raw()
	require.NoError(err)
	header.DedupedMap = *rawDedupedMap

	// also store the deduped map

	buf := bytes.NewBuffer(nil)
	err = dm.serialize(&privKey, LZ4Compression, buf)
	require.NoError(err)

	err = driver.SetHeader(snapshotID, buf)
	require.NoError(err)

	// load header (this will in fact load the deduped map, with backwards compatibility)

	outHeader, err := LoadHeader(snapshotID, driver, &privKey, LZ4Compression)
	require.NoError(err)

	// verify loaded header

	require.Equal(header.DedupedMap.Count, outHeader.DedupedMap.Count)
	require.Subset(header.DedupedMap.Indices, outHeader.DedupedMap.Indices)
	require.Subset(header.DedupedMap.Hashes, outHeader.DedupedMap.Hashes)
}

func testExportPipeline(t *testing.T, driver StorageDriver, dm *DedupedMap) *exportPipeline {
	require := require.New(t)

	compressor, err := NewCompressor(LZ4Compression)
	require.NoError(err)

	var encrypter Encrypter
	if privKey.Defined() {
		encrypter, err = NewEncrypter(&privKey)
		require.NoError(err)
	}

	hasher, err := newKeyedHasher(LZ4Compression, privKey)
	require.NoError(err)

	return &exportPipeline{
		Hasher:        hasher,
		Compressor:    compressor,
		Encrypter:     encrypter,
		StorageDriver: driver,
		DedupedMap:    dm,
	}
}

// ====
// CODE COPIED FROM REMOVED (OLD) CODE!!
// IT IS ONLY HERE TO EMULATE A SERIALIZED AND DIRECTLY STORED DEDUPED MAP!!
// ====

// serialize allows you to write all data of this map in a binary encoded manner,
// to the given writer. The encoded data will be compressed and encrypted before being
// writen to the given writer.
// You can re-load this map in memory using the `DeserializeDedupedMap` function.
func (dm *DedupedMap) serialize(key *CryptoKey, ct CompressionType, dst io.Writer) error {
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

// dedupedMapEncodeFormat defines the structure used to
// encode a deduped map to a binary format.
// See https://github.com/zeebo/bencode for more information.
type dedupedMapEncodeFormat struct {
	Count   int64    `bencode:"c"`
	Indices []int64  `bencode:"i"`
	Hashes  [][]byte `bencode:"h"`
}
