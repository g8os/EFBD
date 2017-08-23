package compress

import (
	"compress/gzip"
	"fmt"

	"github.com/zero-os/0-stor/client/lib/block"
)

// Compressor/decompressor type
const (
	TypeSnappy = "snappy"
	TypeGzip   = "gzip"
	TypeLz4    = "lz4"
)

// Compression level, only apply for gzip
const (
	BestSpeed          = gzip.BestSpeed
	BestCompression    = gzip.BestCompression
	DefaultCompression = gzip.DefaultCompression
	HuffmanOnly        = gzip.HuffmanOnly
)

// Config define compressor and decompressor configuration
type Config struct {
	// Compressor type : TypeSnappy, TypeGzip
	Type string `yaml:"type"`

	// Compression level : only supported for gzip
	// Leave it blank for default value
	Level int `yaml:"level"`
}

// NewWriter returns a new Writer. Writes to the returned writer are compressed and written to w.
func NewWriter(c Config, w block.Writer) (block.Writer, error) {
	switch c.Type {
	case TypeSnappy:
		return newSnappyWriter(w), nil

	case TypeGzip:
		if c.Level == 0 {
			c.Level = DefaultCompression
		}

		return newGzipWriter(w, c.Level), nil

	case TypeLz4:
		return newLz4Writer(w), nil

	default:
		return nil, fmt.Errorf("unsupported compressor type:%v", c.Type)
	}
}

// NewReader returns a new Reader that decompresses from r
func NewReader(c Config) (block.Reader, error) {
	switch c.Type {
	case TypeSnappy:
		return newSnappyReader(), nil

	case TypeGzip:
		return newGzipReader()

	case TypeLz4:
		return newLz4Reader(), nil
	default:
		return nil, fmt.Errorf("unsupported decompressor type:%v", c.Type)
	}
}
