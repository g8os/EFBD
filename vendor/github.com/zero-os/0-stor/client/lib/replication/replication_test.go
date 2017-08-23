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
	numWriter := 10
	data := make([]byte, 4096)
	rand.Read(data)

	// create block writers which do the replication
	for i := 0; i < numWriter; i++ {
		writers = append(writers, block.NewBytesBuffer())
	}

	md, err := meta.New(nil, 0, nil)
	require.Nil(t, err)

	w := NewWriter(writers, Config{Async: async})
	md, err = w.WriteBlock(nil, data, md)
	assert.Nil(t, err)
	assert.Equal(t, numWriter*len(data), int(md.Size()))
	for i := 0; i < numWriter; i++ {
		buff := writers[i].(*block.BytesBuffer)
		assert.Equal(t, data, buff.Bytes())
	}
}
