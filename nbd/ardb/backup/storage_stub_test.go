package backup

import (
	"errors"
	"io"
	"io/ioutil"
	"sync"

	"github.com/zero-os/0-Disk"
)

// newStubDriver creates an in-memory Storage Driver,
// which is to be used for testing purposes only.
func newStubDriver() *stubDriver {
	return &stubDriver{
		dedupedBlocks: make(map[string][]byte),
		headers:       make(map[string][]byte),
	}
}

type stubDriver struct {
	dedupedBlocks map[string][]byte
	headers       map[string][]byte

	bmux, mmux sync.RWMutex
}

// SetDedupedBlock implements StorageDriver.SetDedupedBlock
func (stub *stubDriver) SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	stub.bmux.Lock()
	defer stub.bmux.Unlock()
	stub.dedupedBlocks[string(hash)] = bytes
	return nil
}

// SetHeader implements StorageDriver.SetHeader
func (stub *stubDriver) SetHeader(id string, r io.Reader) error {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	stub.mmux.Lock()
	defer stub.mmux.Unlock()
	stub.headers[id] = bytes
	return nil
}

// GetDedupedBlock implements StorageDriver.GetDedupedBlock
func (stub *stubDriver) GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error {
	stub.bmux.RLock()
	defer stub.bmux.RUnlock()

	bytes, ok := stub.dedupedBlocks[string(hash)]
	if !ok {
		return ErrDataDidNotExist
	}
	n, err := w.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return errors.New("couldn't write full block")
	}
	return nil
}

// GetHeader implements StorageDriver.GetHeader
func (stub *stubDriver) GetHeader(id string, w io.Writer) error {
	stub.mmux.RLock()
	defer stub.mmux.RUnlock()

	bytes, ok := stub.headers[id]
	if !ok {
		return ErrDataDidNotExist
	}
	n, err := w.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return errors.New("couldn't write full header")
	}
	return nil
}

// Close implements StorageDriver.Close
func (stub *stubDriver) Close() error {
	return nil // nothing to do
}
