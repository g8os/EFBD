package backup

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestHashFetcher(t *testing.T) {
	assert := assert.New(t)

	const (
		hashCount = 8
	)

	// create test data
	dedupedMap, ihmapping := generateTestFetcherData(hashCount)
	if !assert.NotNil(dedupedMap) || !assert.NotEmpty(ihmapping) {
		return
	}

	// test fetcher with all hashes

	fetcher := newHashFetcher(dedupedMap)
	if !assert.NotNil(fetcher) {
		return
	}

	for i := 0; i < hashCount; i++ {
		pair, err := fetcher.FetchHash()
		if assert.NoError(err) {
			assert.Equalf(int64(i), pair.Index, "hash: %v", i)
			assert.Equalf(ihmapping[int64(i)], pair.Hash, "hash: %v", i)
		}
	}

	_, err := fetcher.FetchHash()
	assert.Equal(io.EOF, err)

	// test fetcher with half of available blocks

	fetcher.cursor = 0
	fetcher.pairs = fetcher.pairs[:hashCount/2]
	fetcher.length = int64(len(fetcher.pairs))

	for i := 0; i < hashCount/2; i++ {
		pair, err := fetcher.FetchHash()
		if assert.NoError(err) {
			assert.Equalf(int64(i), pair.Index, "hash: %v", i)
			assert.Equalf(ihmapping[int64(i)], pair.Hash, "hash: %v", i)
		}
	}

	_, err = fetcher.FetchHash()
	assert.Equal(io.EOF, err)
}

func TestComputeSnapshotImportSize_EqualTargetBlocks(t *testing.T) {
	assert := assert.New(t)
	assertSnapshotSize := func(index, blockSize int64, expectedSnapshotSize uint64) {
		snapshotSize, err := computeSnapshotImportSize(
			indexHashPair{Index: index}, nil, blockSize,
			importConfig{DstBlockSize: blockSize})
		if assert.NoError(err) {
			assert.Equal(expectedSnapshotSize, snapshotSize)
		}
	}

	assertSnapshotSize(0, 512, 512)
	assertSnapshotSize(0, 1024, 1024)
	assertSnapshotSize(1, 512, 1024)
	assertSnapshotSize(1, 1024, 2048)
	assertSnapshotSize(4, 512, 2560)
}

func TestComputeSnapshotImportSize_BiggerTargetBlocks(t *testing.T) {
	assert := assert.New(t)
	assertSnapshotSize := func(index, srcBlockSize, dstBlockSize int64, expectedSnapshotSize uint64) {
		snapshotSize, err := computeSnapshotImportSize(
			indexHashPair{Index: index}, nil, srcBlockSize,
			importConfig{DstBlockSize: dstBlockSize})
		if assert.NoError(err) {
			assert.Equal(expectedSnapshotSize, snapshotSize)
		}
	}

	assertSnapshotSize(0, 512, 1024, 1024)
	assertSnapshotSize(1, 512, 1024, 1024)
	assertSnapshotSize(2, 512, 1024, 2048)
	assertSnapshotSize(8, 128, 1024, 2048)
	assertSnapshotSize(9, 128, 1024, 2048)
}

func TestComputeSnapshotImportSize_SmallerTargetBlocks(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	driver := newStubDriver()

	compressor, err := NewCompressor(LZ4Compression)
	require.NoError(err)

	var encrypter Encrypter
	if privKey.Defined() {
		encrypter, err = NewEncrypter(&privKey)
		require.NoError(err)
	}

	hasher, err := newKeyedHasher(LZ4Compression, privKey)
	require.NoError(err)

	dm := newDedupedMap()
	pipeline := &exportPipeline{
		Hasher:        hasher,
		Compressor:    compressor,
		Encrypter:     encrypter,
		StorageDriver: driver,
		DedupedMap:    dm,
	}

	assertSnapshotSize :=
		func(index, srcBlockSize, dstBlockSize int64, srcBlock []byte, expectedSnapshotSize uint64) {
			pipeline.WriteBlock(index, srcBlock)
			snapshotSize, err := computeSnapshotImportSize(
				indexHashPair{Index: index, Hash: dm.hashes[index]}, driver, srcBlockSize,
				importConfig{
					DstBlockSize:    dstBlockSize,
					CryptoKey:       privKey,
					CompressionType: LZ4Compression,
				})
			if assert.NoError(err) {
				assert.Equal(expectedSnapshotSize, snapshotSize)
			}
		}

	assertSnapshotSize(0, 4, 2, []byte{0, 1, 0, 0}, 2)
	assertSnapshotSize(0, 4, 2, []byte{0, 1, 1, 0}, 4)
	assertSnapshotSize(0, 8, 2, []byte{0, 1, 1, 0, 0, 0, 1, 0}, 8)
	assertSnapshotSize(0, 8, 2, []byte{0, 1, 1, 0, 0, 1, 0, 0}, 6)
	assertSnapshotSize(0, 8, 2, []byte{0, 1, 1, 0, 0, 0, 0, 0}, 4)
	assertSnapshotSize(0, 8, 2, []byte{0, 1, 0, 0, 0, 0, 0, 0}, 2)
	assertSnapshotSize(1, 8, 2, []byte{0, 1, 1, 0, 0, 1, 0, 0}, 14)
	assertSnapshotSize(1, 8, 2, []byte{0, 1, 1, 0, 0, 0, 0, 0}, 12)
	assertSnapshotSize(1, 8, 2, []byte{0, 1, 0, 0, 0, 0, 0, 0}, 10)
}

func generateTestFetcherData(n int64) (*dedupedMap, map[int64]zerodisk.Hash) {
	dm := newDedupedMap()
	mapping := make(map[int64]zerodisk.Hash)

	for i := int64(0); i < n; i++ {
		hash := zerodisk.NewHash()
		rand.Read(hash)

		dm.SetHash(i, hash)
		mapping[i] = hash
	}

	return dm, mapping
}
