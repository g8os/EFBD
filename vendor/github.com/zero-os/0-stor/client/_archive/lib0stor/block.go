package lib0stor

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

type Block struct {
	hash   []byte //hash of Data
	cipher []byte //key used to encrypte data
	data   []byte //encrypted/compressed data
}

//Encrypt creates a Block from an io.reader
// The block created contains the data from the reader after beeing
// encrypted and compressed
func Encrypt(r io.ReadSeeker) (*Block, error) {

	// hash block
	key, err := hash(r)
	if err != nil {
		return nil, err
	}

	// rewind reader
	_, err = r.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// compress block
	compressed, err := compress(r)
	if err != nil {
		return nil, err
	}

	// encrypt
	encrypted, err := encrypt(compressed, key)
	if err != nil {
		return nil, err
	}

	// hash of the encrypted block
	hEncrypted, err := hash(bytes.NewReader(encrypted))
	if err != nil {
		return nil, err
	}

	return &Block{
		hash:   hEncrypted,
		cipher: key,
		data:   encrypted,
	}, nil
}

// Bytes returns a slice of length b.Len() holding the concatenation
// of the data and crc of the data
func (b *Block) Bytes() []byte {
	buf := &bytes.Buffer{}
	buf.Write(b.data)
	fmt.Fprintf(buf, "%d", crc(b.data))
	return buf.Bytes()
}

// Len return the size of the data+crc
func (b *Block) Len() int {
	return len(b.data) + 4
}

func Decrypt(r io.Reader, key []byte) ([]byte, error) {
	encrypted, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// decrypt
	// compressed, err := decrypt(encrypted, key)
	compressed, err := decrypt(encrypted[:len(encrypted)-1], key)
	if err != nil {
		return nil, err
	}

	// decompress
	data, err := decompress(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}

	h, err := hash(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	if bytes.Compare(key, h) != 0 {
		return nil, fmt.Errorf("integrity check failed: hash mismatch")
	}

	return data, nil
}
