package storage

import (
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
)

// newLBASectorStorage creates a new LBA sector storage
// which writes/reads to/from an ARDB Cluster
func newLBASectorStorage(vdiskID string, cluster ardb.StorageCluster) *lbaSectorStorage {
	return &lbaSectorStorage{
		cluster: cluster,
		vdiskID: vdiskID,
		key:     lbaStorageKey(vdiskID),
	}
}

// lbaSectorStorage is the sector storage implementation,
// used in production, and which writes/reads to/from an ARDB Cluster.
type lbaSectorStorage struct {
	cluster      ardb.StorageCluster
	vdiskID, key string
}

// GetSector implements sectorStorage.GetSector
func (s *lbaSectorStorage) GetSector(index int64) (*lba.Sector, error) {
	reply, err := s.cluster.DoFor(index, ardb.Command(command.HashGet, s.key, index))
	if reply == nil {
		return lba.NewSector(), nil
	}

	data, err := ardb.Bytes(reply, err)
	if err != nil {
		return nil, err
	}

	return lba.SectorFromBytes(data)
}

// SetSector implements sectorStorage.SetSector
func (s *lbaSectorStorage) SetSector(index int64, sector *lba.Sector) error {
	var cmd *ardb.StorageCommand
	if data := sector.Bytes(); data == nil {
		cmd = ardb.Command(command.HashDelete, s.key, index)
	} else {
		cmd = ardb.Command(command.HashSet, s.key, index, data)
	}

	return ardb.Error(s.cluster.DoFor(index, cmd))
}

// lbaStorageKey returns the LBA storage key used for a given deduped vdisk
func lbaStorageKey(vdiskID string) string {
	return lbaStorageKeyPrefix + vdiskID
}

var (
	_ lba.SectorStorage = (*lbaSectorStorage)(nil)
)

const (
	lbaStorageKeyPrefix = "lba:"
)
