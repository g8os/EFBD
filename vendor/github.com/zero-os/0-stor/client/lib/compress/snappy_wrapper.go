package compress

import (
	"github.com/golang/snappy"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

type snappyWriter struct {
	w block.Writer
}

func newSnappyWriter(w block.Writer) *snappyWriter {
	return &snappyWriter{
		w: w,
	}
}

func (sw snappyWriter) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	encoded := snappy.Encode(nil, val)
	md.SetSize(uint64(len(encoded)))
	return sw.w.WriteBlock(key, encoded, md)
}

type snappyReader struct {
}

func newSnappyReader() *snappyReader {
	return &snappyReader{}
}

func (sr *snappyReader) ReadBlock(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
