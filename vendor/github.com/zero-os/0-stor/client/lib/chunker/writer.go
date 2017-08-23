package chunker

import (
	"fmt"
	"io"

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
	var err error
	rd := NewReader(bw.rd, bw.conf)

	var n, totWritten int

	for rd.Next() {
		data := rd.Value()
		fmt.Printf("write key=%v \n", string(chunkKey(key, n)))
		md, err = bw.w.WriteBlock(chunkKey(key, n), data, md)
		if err != nil {
			return md, err
		}
		totWritten += int(md.Size())
		n++
	}
	md, err = meta.New(key, uint64(totWritten), []string{})
	if err != nil {
		return md, err
	}

	md.SetNumOfChunks(uint64(n))
	return md, bw.metaCli.Put(string(key), md)
}
