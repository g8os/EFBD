package slavesync

import (
	"context"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
)

// Manager defines slave syncer manager
type Manager struct {
	syncers      map[string]*slaveSyncer
	configSource config.Source
	mux          sync.Mutex
	privKey      string
	ctx          context.Context
}

// NewManager creates new slave syncer manager
func NewManager(ctx context.Context, configSource config.Source, privKey string) *Manager {
	m := &Manager{
		configSource: configSource,
		syncers:      make(map[string]*slaveSyncer),
		privKey:      privKey,
		ctx:          ctx,
	}
	return m
}

// Create implement SlaveSyncerManager.Create interface
func (m *Manager) Create(vdiskID string) (tlog.SlaveSyncer, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	// check if it already exist
	ss, ok := m.syncers[vdiskID]
	if ok {
		return ss, nil
	}

	// creates if not exist
	ss, err := newSlaveSyncer(m.ctx, m.configSource, vdiskID, m.privKey, m)
	if err != nil {
		log.Errorf("slavesync mgr: failed to create syncer for vdisk: %v, err: %v", vdiskID, err)
		return nil, err
	}
	m.syncers[vdiskID] = ss
	return ss, nil
}

// remove slave syncer for given vdiskID
func (m *Manager) remove(vdiskID string) {
	m.mux.Lock()
	defer m.mux.Unlock()

	delete(m.syncers, vdiskID)
}
