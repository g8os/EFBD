package main

type block struct {
	volumeID  uint32
	sequence  uint64
	lba       uint64
	size      uint32
	data      []byte
	timestamp uint64
}

func newBlock() error {
	return nil
}
