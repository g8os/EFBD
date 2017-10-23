package storage

import (
	"bytes"
	"compress/gzip"
	"io"
	"math/big"
	"sync"
)

// newBitMap creates a new bitMap
func newBitMap() (bm bitMap) {
	return
}

// bitMap represents an auto-expanding bitmap,
// which can be loaded from a string (bytes),
// as well as created from scratch with a certain amount of memory.
type bitMap struct {
	val big.Int
	mux sync.RWMutex
}

// Set the bit at the given position
func (bm *bitMap) Set(pos int) {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	bm.val.SetBit(&bm.val, pos, 1)
}

// Unset the bit at the given position
func (bm *bitMap) Unset(pos int) {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	bm.val.SetBit(&bm.val, pos, 0)
}

// Test the bit at the given position
func (bm *bitMap) Test(pos int) bool {
	bm.mux.RLock()
	defer bm.mux.RUnlock()

	return bm.val.Bit(pos) == 1
}

// Bytes returns the bitMap as a byte slice.
// NOTE that returned content will be gzipped.
func (bm *bitMap) Bytes() ([]byte, error) {
	bm.mux.RLock()
	defer bm.mux.RUnlock()

	// get uncompressed bytes
	input := bm.val.Bytes()

	// prepare gzip compresser
	var buf bytes.Buffer
	compr := gzip.NewWriter(&buf)

	// gzip content
	_, err := compr.Write(input)
	if err != nil {
		return nil, err
	}
	err = compr.Close()
	if err != nil {
		return nil, err
	}

	// return compressed bytes
	return buf.Bytes(), nil
}

// SetBytes as the new internal value of this bit map.
// NOTE that this method expects the input bytes to be gzipped.
func (bm *bitMap) SetBytes(b []byte) error {
	bm.mux.Lock()
	defer bm.mux.Unlock()

	decomp, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	decomp.Multistream(false)
	defer decomp.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, decomp)
	if err != nil {
		return err
	}

	bm.val.SetBytes(buf.Bytes())
	return nil
}
