package pipe

import (
	"io"

	"github.com/zero-os/0-stor/client/config"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// WritePipe defines a pipe of writer
type WritePipe struct {
	w block.Writer
}

// create pipe of block writer
func createBlockWriterPipe(conf *config.Config, iyoToken string, finalWriter block.Writer,
	r io.Reader) (block.Writer, error) {

	nextWriter := finalWriter

	// we create the writer from the end of pipe
	for i := len(conf.Pipes) - 1; i >= 0; i-- {
		pipe := conf.Pipes[i]
		w, err := pipe.CreateBlockWriter(nextWriter, conf.Shards, conf.MetaShards, conf.Protocol,
			conf.Organization, conf.Namespace, iyoToken, r)
		if err != nil {
			return nil, err
		}
		nextWriter = w
	}
	return nextWriter, nil

}

// NewWritePipe create writer pipe
func NewWritePipe(conf *config.Config, iyoToken string, finalWriter block.Writer, r io.Reader) (*WritePipe, error) {
	w, err := createBlockWriterPipe(conf, iyoToken, finalWriter, r)
	if err != nil {
		return nil, err
	}
	return &WritePipe{
		w: w,
	}, nil
}

// WriteBlock implements block.Writer
func (wp WritePipe) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	return wp.w.WriteBlock(key, val, md)
}
