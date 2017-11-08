package tlog

import (
	"time"
)

// SlaveSyncerManager represents object
// which handle slave syncer creation
type SlaveSyncerManager interface {
	// Create creates slave syncer for given
	// vdisk ID
	Create(vdiskID string) (SlaveSyncer, error)
}

// SlaveSyncer represents object
// that sync tlog operation to ndb slave cluster
// for a vdisk
type SlaveSyncer interface {
	// SendAgg send raw aggregation to this slave syncer
	SendAgg(rawAgg []byte)

	// WaitSync wait until given sequence ID being synced
	// to slave cluster
	WaitSync(seq uint64, timeout time.Duration) error

	// Restart restarts this slave syncer.
	// Currently used to simplify config changes handling
	Restart()

	// Stop stops this slave syncer
	Stop()
}
