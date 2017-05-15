package server

type block struct {
	vdiskID   uint32
	sequence  uint64
	lba       uint64
	size      uint32
	data      []byte
	timestamp uint64
}
