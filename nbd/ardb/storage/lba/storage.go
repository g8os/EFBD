package lba

import (
	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// SectorStorage defines the API for a persistent storage,
// used to fetch sectors from which aren't cached yet,
// and to store sectors which are evicted from a cache.
// NOTE: a sectorStorage is not guaranteed to be thread-safe!
type SectorStorage interface {
	// GetSector fetches a sector from a storage,
	// returning an error if this wasn't possible.
	GetSector(index int64) (*Sector, error)

	// SetSector marks a sector persistent,
	// by preparing it to store on a stoage.
	// Note that it is isn't stored until you call
	// the flush function.
	SetSector(index int64, sector *Sector) error
	// Flush flushes all added sectors to the storage.
	Flush() error
}

// ARDBSectorStorage creates a new sector storage
// which writes/reads to/from an ARDB Server
func ARDBSectorStorage(vdiskID string, provider ardb.DataConnProvider) SectorStorage {
	return &ardbSectorStorage{
		provider: provider,
		vdiskID:  vdiskID,
		key:      StorageKey(vdiskID),
	}
}

// ardbSectorStorage is the sector storage implementation,
// used in production, and which writes/reads to/from an ARDB server.
type ardbSectorStorage struct {
	provider     ardb.DataConnProvider
	vdiskID, key string
}

// GetSector implements sectorStorage.GetSector
func (s *ardbSectorStorage) GetSector(index int64) (*Sector, error) {
	conn, err := s.provider.DataConnection(index)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", s.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if s.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  s.vdiskID,
					},
				)
			}
		}
		return nil, err
	}
	defer conn.Close()

	reply, err := conn.Do("HGET", s.key, index)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", s.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if s.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  s.vdiskID,
					},
				)
			}
		}
		return nil, err
	}

	if reply == nil {
		return NewSector(), nil
	}

	data, err := redis.Bytes(reply, err)
	if err != nil {
		return nil, err
	}

	return SectorFromBytes(data)
}

// SetSector implements sectorStorage.SetSector
func (s *ardbSectorStorage) SetSector(index int64, sector *Sector) error {
	// [TODO] see if we should re-enable pipelining again for sending mutliple sectors at once
	// currently it is not possible as the current provider interface has no method
	// which would tell us the storae server used for the given index,
	// without opening a new connection as well.
	// see: https://github.com/zero-os/0-Disk/issues/483
	conn, err := s.provider.DataConnection(index)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", s.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if s.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  s.vdiskID,
					},
				)
			}
		}
		return err
	}
	defer conn.Close()

	data := sector.Bytes()
	if data == nil {
		_, err = conn.Do("HDEL", s.key, index)
	} else {
		_, err = conn.Do("HSET", s.key, index, data)
	}
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", s.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if s.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  s.vdiskID,
					},
				)
			}
		}
		return err
	}

	return nil
}

// Flush implements sectorStorage.Flush
func (s *ardbSectorStorage) Flush() error {
	// nothing to do for now...
	// see body of `(*ardbSectorStorage).SetSector` to know why
	return nil
}
