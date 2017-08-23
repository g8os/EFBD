package block

import (
	"errors"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

var (
	ErrEmptyShard = errors.New("empty shards")
)

type Config struct {
	//Shards []string `yaml:"-"`
}

// Writer is meta client that implements block.Writer interface
type Writer struct {
	client *meta.Client
	w      block.Writer
}

// NewWriter creates new block Writer
func NewWriter(w block.Writer, conf Config, shards []string) (*Writer, error) {
	cli, err := meta.NewClient(shards)
	if err != nil {
		return nil, err
	}
	return &Writer{
		client: cli,
		w:      w,
	}, nil
}

// WriteBlock implements block.Writer interface.
// The value is encoded metadata
func (w *Writer) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	if err := w.client.Put(string(key), md); err != nil {
		return md, err
	}
	return w.w.WriteBlock(key, val, md)
}

type Reader struct {
	client *meta.Client
}

func NewReader(conf Config, shards []string) (*Reader, error) {
	cli, err := meta.NewClient(shards)
	if err != nil {
		return nil, err
	}
	return &Reader{
		client: cli,
	}, nil
}

func (r *Reader) ReadBlock(key []byte) ([]byte, error) {
	md, err := r.client.Get(string(key))
	if err != nil {
		return nil, err
	}
	return md.Bytes()
}
