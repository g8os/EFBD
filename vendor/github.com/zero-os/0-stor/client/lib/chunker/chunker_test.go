package chunker

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestChunkerEven test chunker with data length == multiply of chunkSize
func TestChunkerEven(t *testing.T) {
	dataLen := 100
	conf := Config{
		ChunkSize: 10,
	}

	data := make([]byte, dataLen)
	c := NewChunker(conf)
	c.Chunk(data)
	testSplitterEven(t, c, dataLen, conf.ChunkSize)
}

func TestReaderEven(t *testing.T) {
	dataLen := 100
	conf := Config{
		ChunkSize: 10,
	}

	data := make([]byte, dataLen)
	c := NewReader(bytes.NewReader(data), conf)
	testSplitterEven(t, c, dataLen, conf.ChunkSize)
}

func testSplitterEven(t *testing.T, s splitter, dataLen, chunkSize int) {
	chunkNum := 0

	for s.Next() {
		chunkNum++
		val := s.Value()
		assert.Equal(t, chunkSize, len(val))
	}
	assert.Equal(t, dataLen/chunkSize, chunkNum)
}

func TestChunkerNotEven(t *testing.T) {
	dataLen := 99
	conf := Config{
		ChunkSize: 10,
	}

	data := make([]byte, dataLen)

	c := NewChunker(conf)
	c.Chunk(data)
	testSplitterNotEven(t, c, dataLen, conf.ChunkSize)
}

func TestReaderNotEven(t *testing.T) {
	dataLen := 99
	conf := Config{
		ChunkSize: 10,
	}

	data := make([]byte, dataLen)

	c := NewReader(bytes.NewReader(data), conf)
	testSplitterNotEven(t, c, dataLen, conf.ChunkSize)
}

// testSplitterNotEven test splitter with data length !=  multiply of chunkSize
func testSplitterNotEven(t *testing.T, s splitter, dataLen, chunkSize int) {
	expectedChunkNum := (dataLen / chunkSize) + 1
	chunkNum := 0

	for s.Next() {
		chunkNum++
		val := s.Value()
		if chunkNum != expectedChunkNum {
			assert.Equal(t, chunkSize, len(val))
		} else {
			assert.Equal(t, dataLen%chunkSize, len(val))
		}
	}
	assert.Equal(t, (dataLen/chunkSize)+1, chunkNum)
}
