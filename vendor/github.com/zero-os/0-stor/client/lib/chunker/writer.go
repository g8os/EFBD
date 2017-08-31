package chunker

import (
	"fmt"
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// BlockWriter is chunker which implement block.Writer interface
type BlockWriter struct {
	conf    Config
	w       block.Writer
	metaCli *meta.Client
	rd      io.Reader
}

func chunkKey(key []byte, idx int) []byte {
	return []byte(fmt.Sprintf("%v_%v", string(key), idx))
}

func NewBlockWriter(w block.Writer, conf Config, metaShards []string, r io.Reader) (*BlockWriter, error) {
	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		conf:    conf,
		w:       w,
		metaCli: metaCli,
		rd:      r,
	}, nil
}

// WriteBlock implements block.Writer.WriteBlock interface
// val is being ignore here
func (bw *BlockWriter) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	var (
		n   int
		err error
	)
	rd := NewReader(bw.rd, bw.conf)

	for rd.Next() {
		data := rd.Value()
		log.Debugf("write key=%v \n", string(chunkKey(key, n)))
		md, err = bw.w.WriteBlock(chunkKey(key, n), data, md)
		if err != nil {
			return md, err
		}

		// add a new chunk to the metadata
		chunk := md.GetChunk(key)
		chunk.Shards = []string{} // the final location is not known yet at this stage
		chunk.Size = uint64(len(data))

		n++
	}

	return md, nil
}
