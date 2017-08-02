package config

import (
	"errors"
	"sync"
	"syscall"
)

// NewStubSource create a new stub source, for testing purposes
func NewStubSource() *StubSource {
	source := new(StubSource)
	source.fileSource.path = "/tests/in/memory"
	source.fileSource.reader = source.readConfig

	return source
}

// StubSource is a modified file source using an internal stored config in memory
// used for testing purposes only.
type StubSource struct {
	fileSource
	cfg *fileFormatCompleteConfig
	mux sync.Mutex
}

// TriggerReload triggers a reload of the config of this source.
func (s *StubSource) TriggerReload() {
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
}

// SetVdiskConfig is a utility function to set a vdisk config, thread-safe.
func (s *StubSource) SetVdiskConfig(vdiskID string, cfg *fileFormatVdiskConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.cfg == nil {
		s.cfg = &fileFormatCompleteConfig{
			Vdisks: make(map[string]fileFormatVdiskConfig),
		}
	} else if s.cfg.Vdisks == nil {
		s.cfg.Vdisks = make(map[string]fileFormatVdiskConfig)
	}

	if cfg == nil {
		delete(s.cfg.Vdisks, vdiskID)
	} else {
		s.cfg.Vdisks[vdiskID] = *cfg
	}
}

// SetStorageCluster is a utility function to set a storage cluster config, thread-safe.
func (s *StubSource) SetStorageCluster(clusterID string, cfg *StorageClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.cfg == nil {
		s.cfg = &fileFormatCompleteConfig{
			StorageClusters: make(map[string]StorageClusterConfig),
		}
	} else if s.cfg.StorageClusters == nil {
		s.cfg.StorageClusters = make(map[string]StorageClusterConfig)
	}

	if cfg == nil {
		delete(s.cfg.StorageClusters, clusterID)
	} else {
		s.cfg.StorageClusters[clusterID] = *cfg
	}
}

// SetTlogCluster is a utility function to set a tlog cluster config, thread-safe.
func (s *StubSource) SetTlogCluster(clusterID string, cfg *TlogClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.cfg == nil {
		s.cfg = &fileFormatCompleteConfig{
			TlogClusters: make(map[string]TlogClusterConfig),
		}
	} else if s.cfg.StorageClusters == nil {
		s.cfg.TlogClusters = make(map[string]TlogClusterConfig)
	}

	if cfg == nil {
		delete(s.cfg.TlogClusters, clusterID)
	} else {
		s.cfg.TlogClusters[clusterID] = *cfg
	}
}

// readConfig
func (s *StubSource) readConfig(string) ([]byte, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.cfg == nil {
		return nil, errors.New("stub: no test config defined")
	}

	return serializeConfigReply(s.cfg, nil)
}
