package backup

import (
	"crypto/rand"
	"io"
	"testing"

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

func generateTestFetcherData(n int64) (*DedupedMap, map[int64]zerodisk.Hash) {
	dm := NewDedupedMap()
	mapping := make(map[int64]zerodisk.Hash)

	for i := int64(0); i < n; i++ {
		hash := zerodisk.NewHash()
		rand.Read(hash)

		dm.SetHash(i, hash)
		mapping[i] = hash
	}

	return dm, mapping
}
