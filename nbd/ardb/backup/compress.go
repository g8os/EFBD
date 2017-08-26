package backup

import (
	"errors"
	"io"

	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

const (
	// LZ4Compression represents the LZ4 Compression Type,
	// and is also the Default (nil) value of the Compression Type.
	// See https://github.com/pierrec/lz4 for more information.
	LZ4Compression CompressionType = iota
	// XZCompression represents the XZ Compression Type.
	// See https://github.com/ulikunitz/xz for more information.
	XZCompression
)

// CompressionType defines a type of a compression.
type CompressionType uint8

// String implements Stringer.String
func (ct *CompressionType) String() string {
	switch *ct {
	case LZ4Compression:
		return lz4CompressionStr
	case XZCompression:
		return xzCompressionStr
	default:
		return ""
	}
}

// Set implements Flag.Set
func (ct *CompressionType) Set(str string) error {
	switch str {
	case lz4CompressionStr:
		*ct = LZ4Compression
	case xzCompressionStr:
		*ct = XZCompression
	default:
		return errUnknownCompressionType
	}

	return nil
}

// Type implements PValue.Type
func (ct *CompressionType) Type() string {
	return "CompressionType"
}

func (ct CompressionType) validate() error {
	switch ct {
	case LZ4Compression, XZCompression:
		return nil
	default:
		return errUnknownCompressionType
	}
}

const (
	lz4CompressionStr = "lz4"
	xzCompressionStr  = "xz"
)

var (
	errUnknownCompressionType = errors.New("unknown compression type")
)

// NewCompressor automatically creates the correct compressor (if possible),
// based on the given compression type.
func NewCompressor(ct CompressionType) (Compressor, error) {
	switch ct {
	case LZ4Compression:
		return LZ4Compressor(), nil
	case XZCompression:
		return XZCompressor(), nil
	default:
		return nil, errUnknownCompressionType
	}
}

// NewDecompressor automatically creates the correct decompression (if possible),
// based on the given compression type.
func NewDecompressor(ct CompressionType) (Decompressor, error) {
	switch ct {
	case LZ4Compression:
		return LZ4Decompressor(), nil
	case XZCompression:
		return XZDecompressor(), nil
	default:
		return nil, errUnknownCompressionType
	}
}

// LZ4Compressor creates an LZ4 Compressor.
// See `LZ4Compression` for more information.
func LZ4Compressor() Compressor {
	return lz4Compressor{}
}

// LZ4Decompressor creates an LZ4 Decompressor.
// See `LZ4Compression` for more information.
func LZ4Decompressor() Decompressor {
	return lz4Decompressor{}
}

// XZCompressor creates an XZ Compressor.
// See `XZCompression` for more information.
func XZCompressor() Compressor {
	return xzCompressor{}
}

// XZDecompressor creates an XZ Decompressor.
// See `XZCompression` for more information.
func XZDecompressor() Decompressor {
	return xzDecompressor{}
}

// Compressor defines the API for any type of compressor.
type Compressor interface {
	Compress(src io.Reader, dst io.Writer) error
}

// Decompressor defines the API for any type of decompressor.
type Decompressor interface {
	Decompress(src io.Reader, dst io.Writer) error
}

// lz4 compression
// see https://github.com/pierrec/lz4 for more information.

type lz4Compressor struct{}

func (c lz4Compressor) Compress(src io.Reader, dst io.Writer) error {
	w := lz4.NewWriter(dst)
	w.Header.BlockChecksum = true

	n, err := w.ReadFrom(src)
	if n <= 0 {
		w.Close()
		return errors.New("couldn't (LZ4) compress any data")
	}
	if err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

type lz4Decompressor struct{}

func (d lz4Decompressor) Decompress(src io.Reader, dst io.Writer) error {
	r := lz4.NewReader(src)
	r.Header.BlockChecksum = true

	n, err := r.WriteTo(dst)
	if n <= 0 {
		return errors.New("couldn't (LZ4) decompress any data")
	}
	return err
}

// xz compression
// see https://github.com/ulikunitz/xz for more information.

type xzCompressor struct{}

func (c xzCompressor) Compress(src io.Reader, dst io.Writer) error {
	w, err := xz.NewWriter(dst)
	if err != nil {
		return err
	}

	n, err := io.Copy(w, src)
	if n <= 0 {
		return errors.New("couldn't (XZ) compress any data")
	}
	if err != nil {
		w.Close()
		return err
	}

	return w.Close()
}

type xzDecompressor struct{}

func (d xzDecompressor) Decompress(src io.Reader, dst io.Writer) error {
	r, err := xz.NewReader(src)
	if err != nil {
		return err
	}

	n, err := io.Copy(dst, r)
	if n <= 0 {
		return errors.New("couldn't (XZ) decompress any data")
	}
	return err
}
