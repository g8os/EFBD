package compress

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

type gzipWriter struct {
	w     block.Writer
	level int
}

func newGzipWriter(w block.Writer, level int) *gzipWriter {
	return &gzipWriter{
		w:     w,
		level: level,
	}
}

func (gw gzipWriter) WriteBlock(key, p []byte, md *meta.Meta) (*meta.Meta, error) {
	buf := new(bytes.Buffer)
	written, err := func() (int, error) {
		w, err := gzip.NewWriterLevel(buf, gw.level)
		if err != nil {
			return 0, err
		}

		var written int
		for {
			n, err := w.Write(p)
			if err != nil {
				return n, err
			}
			if n == 0 {
				break
			}
			p = p[n:]
			written += n
		}

		if err := w.Flush(); err != nil {
			return written, err
		}
		return written, w.Close()
	}()

	if err != nil {
		return md, err
	}

	// update chunk size in metadata
	chunk := md.GetChunk(key)
	chunk.Size = uint64(written)

	return gw.w.WriteBlock(key, buf.Bytes(), md)
}

type gzipReader struct {
	*gzip.Reader
}

func newGzipReader() (*gzipReader, error) {
	return &gzipReader{}, nil
}

// ReadBlock implements block.Reader
func (gr gzipReader) ReadBlock(data []byte) ([]byte, error) {
	br := bytes.NewReader(data)
	rd, err := gzip.NewReader(br)
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	b, err := ioutil.ReadAll(rd)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return b, nil
}
