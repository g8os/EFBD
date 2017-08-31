package replication

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

func TestReplication(t *testing.T) {
	t.Run("async", func(t *testing.T) {
		testReplication(t, true)
	})
	t.Run("sync", func(t *testing.T) {
		testReplication(t, false)
	})
}

func testReplication(t *testing.T, async bool) {
	var writers []block.Writer
	var err error
	numWriter := 1
	data := make([]byte, 4096)
	rand.Read(data)
	key := []byte("testkey")

	// create block writers which do the replication
	for i := 0; i < numWriter; i++ {
		writers = append(writers, block.NewBytesBuffer())
	}

	md := meta.New(key)

	w := NewWriter(writers, Config{Async: async})
	md, err = w.WriteBlock(key, data, md)
	require.NoError(t, err)

	assert.Equal(t, len(data), int(md.Size()), "size of the file in metadata is not valid")
	// assert.Equal(t, len(writers), len(md.Chunks[0].Shards), "shard number is not valid")
	for i := 0; i < numWriter; i++ {
		buff := writers[i].(*block.BytesBuffer)
		assert.Equal(t, data, buff.Bytes(), "content of the replication is not valid")
	}
	for _, chunk := range md.Chunks {
		assert.EqualValues(t, len(data), chunk.Size, "size of the chunks is not valid")
	}
}
