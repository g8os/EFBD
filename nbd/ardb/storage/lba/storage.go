package lba

import (
	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// sectorStorage defines the API for a persistent storage,
// used to fetch sectors from which aren't cached yet,
// and to store sectors which are evicted from a cache.
// NOTE: a sectorStorage is not guaranteed to be thread-safe!
type sectorStorage interface {
	// GetSector fetches a sector from a storage,
	// returning an error if this wasn't possible.
	GetSector(index int64) (*sector, error)

	// SetSector marks a sector persistent,
	// by preparing it to store on a stoage.
	// Note that it is isn't stored until you call
	// the flush function.
	SetSector(index int64, sector *sector) error
	// Flush flushes all added sectors to the storage.
	Flush() error
}

// newARDBSectorStorage creates a new sector storage which
// writes/reads to/from an ARDB server.
func newARDBSectorStorage(key string, provider ardb.DataConnProvider) *ardbSectorStorage {
	return &ardbSectorStorage{
		provider: provider,
		key:      key,
	}
}

// ardbSectorStorage is the sector storage implementation,
// used in production, and which writes/reads to/from an ARDB server.
type ardbSectorStorage struct {
	provider ardb.DataConnProvider
	key      string
}

// GetSector implements sectorStorage.GetSector
func (s *ardbSectorStorage) GetSector(index int64) (*sector, error) {
	conn, err := s.provider.DataConnection(index)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	reply, err := conn.Do("HGET", s.key, index)
	if err != nil {
		return nil, err
	}

	if reply == nil {
		return newSector(), nil
	}

	data, err := redis.Bytes(reply, err)
	if err != nil {
		return nil, err
	}

	return sectorFromBytes(data)
}

// SetSector implements sectorStorage.SetSector
func (s *ardbSectorStorage) SetSector(index int64, sector *sector) error {
	// [TODO] see if we should re-enable pipelining again for sending mutliple sectors at once
	// currently it is not possible as the current provider interface has no method
	// which would tell us the storae server used for the given index,
	// without opening a new connection as well.
	// see: https://github.com/zero-os/0-Disk/issues/483
	conn, err := s.provider.DataConnection(index)
	if err != nil {
		return err
	}
	defer conn.Close()

	data := sector.Bytes()
	if data == nil {
		_, err = conn.Do("HDEL", s.key, index)
		return err
	}
	_, err = conn.Do("HSET", s.key, index, data)
	return err
}

// Flush implements sectorStorage.Flush
func (s *ardbSectorStorage) Flush() error {
	// nothing to do for now...
	// see body of `(*ardbSectorStorage).SetSector` to know why
	return nil
}
