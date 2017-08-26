package testdata

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

// ReadAllLedeBlocks reads all blocks for a lede
func ReadAllLedeBlocks() (map[int64][]byte, error) {
	file, err := os.Open(ledeImagePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gr, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	rawContent, err := ioutil.ReadAll(gr)
	if err != nil {
		return nil, err
	}

	blocks := make(map[int64][]byte)
	dataLength := int64(len(rawContent))
	for i := int64(0); i < dataLength; i += ledeIndexBlockPairSize {
		data := rawContent[i : i+ledeIndexBlockPairSize]
		index, err := strconv.ParseInt(string(data[:8]), 10, 64)
		if err != nil {
			return nil, err
		}

		blocks[index] = data[8:]
	}

	return blocks, nil
}

// LedeImageBlockSize defines the block size of the test lede image
const LedeImageBlockSize = 4096

const ledeIndexBlockPairSize = 8 + LedeImageBlockSize

var (
	ledeImagePath = path.Join(testDataPath, "ledeimg.gzip")
	testDataPath  = path.Join(os.Getenv("GOPATH"), "src/github.com/zero-os/0-Disk/testdata")
)
