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
	sectors      map[int64][]byte
	flushSectors map[int64][]byte
	mux          sync.RWMutex
}

// GetSector implements sectorStorage.GetSector
func (s *stubSectorStorage) GetSector(index int64) (*sector, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()
	bytes, ok := s.sectors[index]
	if !ok {
		return newSector(), nil
	}
	return sectorFromBytes(bytes)
}

// SetSector implements sectorStorage.SetSector
func (s *stubSectorStorage) SetSector(index int64, sector *sector) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.flushSectors == nil {
		s.flushSectors = make(map[int64][]byte)
	}

	source := sector.Bytes()
	destination := make([]byte, len(source))
	copy(destination, source)
	s.flushSectors[index] = destination
	return nil
}

// Flush implements sectorStorage.Flush
func (s *stubSectorStorage) Flush() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.flushSectors == nil {
		return nil // nothing to do
	}

	for index, bytes := range s.flushSectors {
		s.sectors[index] = bytes
	}
	s.flushSectors = nil
	return nil
}
