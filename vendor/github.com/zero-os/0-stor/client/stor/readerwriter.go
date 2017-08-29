package stor

import (
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// Writer is 0-stor client that implements block.Writer interface
// It writes to random server first and then try all other
// servers if it failed
type Writer struct {
	sc      *ShardsClient
	metaCli *meta.Client
	w       block.Writer
}

// NewWriter creates new writer
func NewWriter(w block.Writer, conf Config, shards, metaShards []string, org, namespace, iyoToken string) (*Writer, error) {
	sc, err := NewShardsClient(conf, shards, org, namespace, iyoToken)
	if err != nil {
		return nil, err
	}

	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}
	return &Writer{
		w:       w,
		sc:      sc,
		metaCli: metaCli,
	}, nil
}

// WriteBlock implements block.Writer.WriteBlock interface
// It creates object in 0-stor server with given key as id and value as data.
// It write the key-metadata to the underlying writer
func (w *Writer) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	var err error

	md, err = w.sc.ObjectCreate(key, val, nil)
	if err != nil {
		return md, err
	}

	// write metadata to the meta client &  underlying writer
	if err := w.metaCli.Put(string(key), md); err != nil {
		return md, err
	}

	mdBytes, err := md.Bytes()
	if err != nil {
		return md, err
	}

	return w.w.WriteBlock(key, mdBytes, md)
}

// Reader is 0-stor client that implements block.Reader interface
type Reader struct {
	metaCli *meta.Client
	sc      *ShardsClient
}

// NewReader creates new Reader
func NewReader(conf Config, shards, metaShards []string, org, namespace, iyoToken string) (*Reader, error) {
	sc, err := NewShardsClient(conf, shards, org, namespace, iyoToken)
	if err != nil {
		return nil, err
	}

	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	return &Reader{
		sc:      sc,
		metaCli: metaCli,
	}, nil
}

// ReadBlock implements block.Reader
// the key is metadata key
func (r *Reader) ReadBlock(key []byte) ([]byte, error) {
	md, err := r.metaCli.Get(string(key))
	if err != nil {
		return nil, err
	}
	return r.sc.ObjectGet(md)
}
