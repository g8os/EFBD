package lba

import (
	"sync"
)

func newStubSectorStorage() *stubSectorStorage {
	return &stubSectorStorage{
		sectors: make(map[int64][]byte),
	}
}

// stubSectorStorage is the sector storage implementation,
// used for LBA unit tests.
type stubSectorStorage struct {
	sectors map[int64][]byte
	mux     sync.RWMutex
}

// GetSector implements sectorStorage.GetSector
func (s *stubSectorStorage) GetSector(index int64) (*Sector, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()
	bytes, ok := s.sectors[index]
	if !ok {
		return NewSector(), nil
	}
	return SectorFromBytes(bytes)
}

// SetSector implements sectorStorage.SetSector
func (s *stubSectorStorage) SetSector(index int64, sector *Sector) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	source := sector.Bytes()
	destination := make([]byte, len(source))
	copy(destination, source)
	s.sectors[index] = destination
	return nil
}
