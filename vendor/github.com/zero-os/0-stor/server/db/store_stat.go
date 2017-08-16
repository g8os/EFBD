package db

import (
	"bytes"
	"encoding/binary"
)

type StoreStat struct {
	// Size available = free disk space - reserved space (in bytes)
	SizeAvailable uint64 `json:"size_available" validate:"min=1,nonzero"`
	// SizeUsed = sum of all reservation size (in bytes)
	SizeUsed uint64 `json:"size_used" validate:"min=1,nonzero"`
}

func (s *StoreStat) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, s.SizeAvailable)
	binary.Write(buf, binary.LittleEndian, s.SizeUsed)
	return buf.Bytes(), nil
}

func (s *StoreStat) Decode(data []byte) error {
	s.SizeAvailable = binary.LittleEndian.Uint64(data[:8])
	s.SizeUsed = binary.LittleEndian.Uint64(data[8:16])
	return nil
}
