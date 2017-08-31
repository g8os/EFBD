package chunker

import (
	"bufio"
	"io"

	log "github.com/Sirupsen/logrus"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// BlockReader is chunker which implements block.Reader interface
type BlockReader struct {
	conf       Config
	fileWriter *bufio.Writer
}

// NewBlockReader creates new chunker blockreader
func NewBlockReader(conf Config) (*BlockReader, error) {

	return &BlockReader{
		conf: conf,
	}, nil
}

// Restore restores the chunked data and write the result to the given io.Writer
func (br *BlockReader) Restore(key []byte, w io.Writer, rd block.Reader, md *meta.Meta) error {
	br.fileWriter = bufio.NewWriter(w)

	for _, chunk := range md.Chunks {
		log.Debugf("read block : %v\n", chunk.Key)
		if _, err := rd.ReadBlock(chunk.Key); err != nil {
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
