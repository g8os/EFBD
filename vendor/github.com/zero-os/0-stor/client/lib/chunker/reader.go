package chunker

import (
	"bufio"
	"fmt"
	"io"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// BlockReader is chunker which implements block.Reader interface
type BlockReader struct {
	conf       Config
	metaCli    *meta.Client
	fileWriter *bufio.Writer
}

// NewBlockReader creates new chunker blockreader
func NewBlockReader(conf Config, metaShards []string) (*BlockReader, error) {
	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		conf:    conf,
		metaCli: metaCli,
	}, nil
}

// Restore restores the chunked data and write the result to the given io.Writer
func (br *BlockReader) Restore(key []byte, w io.Writer, rd block.Reader) error {
	br.fileWriter = bufio.NewWriter(w)

	md, err := br.metaCli.Get(string(key))
	if err != nil {
		return err
	}

	for i := 0; i < int(md.NumOfChunks()); i++ {
		fmt.Printf("read block : %v\n", string(chunkKey(key, i)))
		if _, err := rd.ReadBlock(chunkKey(key, i)); err != nil {
			return err
		}
	}
	return br.fileWriter.Flush()
}

// ReadBlock implements block.Reader.ReadBlock
func (br *BlockReader) ReadBlock(data []byte) ([]byte, error) {
	_, err := br.fileWriter.Write(data)
	return nil, err
}
