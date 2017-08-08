package backup

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
)

func TestHashAsDirAndFile(t *testing.T) {
	assert := assert.New(t)

	for i := 0; i < 32; i++ {
		hash := make([]byte, zerodisk.HashSize)
		rand.Read(hash)

		dir, file, ok := hashAsDirAndFile(hash)
		if assert.True(ok) {
			expected := bytesToString(hash[0:2]) + "/" + bytesToString(hash[2:4])
			assert.Equal(expected, dir)
			expected = bytesToString(hash[4:])
			assert.Equal(expected, file)
		}
	}
}

func TestHashBytesToString(t *testing.T) {
	assert := assert.New(t)

	for x1 := byte(0); x1 < 255; x1++ {
		x2 := 255 - x1
		x3 := (x1 + 5) % 255
		bs := []byte{x1, x2, x3}
		assert.Equal(bytesToString(bs), hashBytesToString(bs))
	}
}

func bytesToString(bs []byte) (str string) {
	for _, b := range bs {
		str += fmt.Sprintf("%02X", b)
	}
	return
}

func TestDirCache(t *testing.T) {
	assert := assert.New(t)

	dc := newDirCache()
	if !assert.NotNil(dc) {
		return
	}

	assert.False(dc.CheckDir("foo"))
	dc.AddDir("foo")
	assert.True(dc.CheckDir("foo"))
	assert.False(dc.CheckDir("bar"))
	dc.AddDir("bar")
	assert.True(dc.CheckDir("bar"))
}
