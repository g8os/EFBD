package distribution

import (
	"fmt"
	"io"
	"io/ioutil"
	//"github.com/zero-os/0-stor/client/itsyouonline"
)

// Distributor distribute the data to the given outputs
type Distributor struct {
	enc     *Encoder
	writers []io.Writer
}

// Config defines distribution's configuration
type Config struct {
	Data   int `yaml:"data"`   // number of data shards
	Parity int `yaml:"parity"` // number of parity shards
}

// NumPieces returns total number of pieces given the configuration
func (c Config) NumPieces() int {
	return c.Data + c.Parity
}

// NewDistributor creates new distribution
func NewDistributor(writers []io.Writer, conf Config) (*Distributor, error) {
	if len(writers) != conf.Data+conf.Parity {
		return nil, fmt.Errorf("invalid number of writers: %v expected: %v", len(writers), conf.NumPieces())
	}

	enc, err := NewEncoder(conf.Data, conf.Parity)
	if err != nil {
		return nil, err
	}

	return &Distributor{
		enc:     enc,
		writers: writers,
	}, nil
}

// Write writes blocks to the given output writers
func (d Distributor) Write(data []byte) (int, error) {
	var written int

	encoded, err := d.enc.Encode(data)
	if err != nil {
		return written, err
	}

	for i, data := range encoded {
		n, err := d.writers[i].Write(data)
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// Restorer restore the distributed data from the given readers
type Restorer struct {
	dec     *Decoder
	readers []io.Reader
}

// NewRestorer creates restorer from the given reader.
func NewRestorer(readers []io.Reader, conf Config) (*Restorer, error) {
	if len(readers) != conf.NumPieces() {
		return nil, fmt.Errorf("invalid number of readers: %v expected: %v", len(readers), conf.NumPieces())
	}

	dec, err := NewDecoder(conf.Data, conf.Parity)
	if err != nil {
		return nil, err
	}
	return &Restorer{
		dec:     dec,
		readers: readers,
	}, nil
}

// Read restores the data from the underlying reader.
// length of the decoded argument must be the same as lenght of
// the original data
func (r *Restorer) Read(decoded []byte) (int, error) {
	origLen := len(decoded)
	chunks := make([][]byte, r.dec.k+r.dec.m)
	chunkLen := getPaddedLen(origLen, r.dec.k) / r.dec.k

	// read all chunks from the underlying reader
	for i, reader := range r.readers {
		data, err := ioutil.ReadAll(reader)
		if err != nil || len(data) != chunkLen {
			// error and invalid lenght are marked as data lost!
		} else {
			chunks[i] = data
		}
	}

	// decode
	res, err := r.dec.Decode(chunks, origLen)
	if err != nil || len(res) != origLen {
		return len(res), err
	}

	copy(decoded, res)
	return len(res), nil
}
