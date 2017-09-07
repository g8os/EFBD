package chunker

import (
	"io"
)

// splitter is the interface for the splitter/chunker
type splitter interface {
	Next() bool
	Value() []byte
}

// Chunker splits data into smaller blocks of a fixed size.
type Chunker struct {
	data      []byte
	chunkSize int
}

// Config defines chunker configuration
type Config struct {
	ChunkSize int    `yaml:"chunkSize" validate:"min=0"`
	DestPath  string `yaml:"-"`
}

// NewChunker creates chunker for given data and chunk size
func NewChunker(conf Config) *Chunker {
	return &Chunker{
		chunkSize: conf.ChunkSize,
	}
}

func (c *Chunker) Chunk(data []byte) {
	c.data = data
}

// Next return true if there is still chunk left
// It doesn't advance the iterator's pointer
func (c *Chunker) Next() bool {
	return len(c.data) > 0
}

// Value returns the current chunk
func (c *Chunker) Value() []byte {
	if len(c.data) < c.chunkSize {
		c.chunkSize = len(c.data)
	}
	chunk := c.data[:c.chunkSize]
	c.data = c.data[c.chunkSize:]
	return chunk
}

// Reader implements io.Reader interface for the splitter
type Reader struct {
	rd        io.Reader
	chunkSize int
	curChunk  []byte
}

// NewReader creates new splitter reader
func NewReader(r io.Reader, conf Config) *Reader {
	return &Reader{
		rd:        r,
		chunkSize: conf.ChunkSize,
		curChunk:  make([]byte, conf.ChunkSize),
	}
}

// Next return true if there is still chunk left.
// It also advanced the iterator's pointer
func (r *Reader) Next() bool {
	n, err := r.rd.Read(r.curChunk)
	if err != nil {
		return false
	}
	if n < r.chunkSize {
		r.curChunk = r.curChunk[:n]
	}
	return true
}

// Value returns current iterator value
func (r *Reader) Value() []byte {
	// out := make([]byte, len(r.curChunk))
	// copy(out, r.curChunk)
	// return out
	return r.curChunk
}
