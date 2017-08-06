package config

import (
	"context"
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

	cfg *FileFormatCompleteConfig
	mux sync.Mutex

	watchCounter    int
	watchCounterMux sync.RWMutex

	invalidConfigSender chan Key
}

// Watch implements Source.Watch
func (s *StubSource) Watch(ctx context.Context, key Key) (<-chan []byte, error) {
	s.watchCounterMux.Lock()
	s.watchCounter++
	s.watchCounterMux.Unlock()
	go func() {
		<-ctx.Done()
		s.watchCounterMux.Lock()
		s.watchCounter--
		s.watchCounterMux.Unlock()
	}()
	return s.fileSource.Watch(ctx, key)
}

// Close implements SourceCloser.Close
func (s *StubSource) Close() error {
	if s.invalidConfigSender != nil {
		close(s.invalidConfigSender)
		s.invalidConfigSender = nil
	}

	return s.fileSource.Close()
}

// MarkInvalidKey implements Source.MarkInvalidKey
func (s *StubSource) MarkInvalidKey(key Key, vdiskID string) {
	if s.invalidConfigSender != nil {
		s.invalidConfigSender <- key
	}
	s.fileSource.MarkInvalidKey(key, vdiskID)
}

// InvalidKey can be used to get a channel to wait for
// an incoming ivnalid key.
func (s *StubSource) InvalidKey() <-chan Key {
	if s.invalidConfigSender == nil {
		s.invalidConfigSender = make(chan Key, 1)
	}

	return s.invalidConfigSender
}

// SetVdiskConfig is a utility function to set a vdisk config, thread-safe.
func (s *StubSource) SetVdiskConfig(vdiskID string, cfg *VdiskStaticConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if cfg == nil {
		delete(s.cfg.Vdisks, vdiskID)
		return
	}

	vdiskCfg.BlockSize = cfg.BlockSize
	vdiskCfg.Size = cfg.Size
	vdiskCfg.VdiskType = cfg.Type
	vdiskCfg.ReadOnly = cfg.ReadOnly
	vdiskCfg.TemplateVdiskID = cfg.TemplateVdiskID

	s.cfg.Vdisks[vdiskID] = vdiskCfg
}

// SetPrimaryStorageCluster is a utility function to set a primary storage cluster config, thread-safe.
func (s *StubSource) SetPrimaryStorageCluster(vdiskID, clusterID string, cfg *StorageClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	if cfg != nil {
		s.setStorageCluster(clusterID, cfg)
	}

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if vdiskCfg.NBD == nil {
		vdiskCfg.NBD = &VdiskNBDConfig{
			StorageClusterID: clusterID,
		}
	} else {
		vdiskCfg.NBD.StorageClusterID = clusterID
	}

	s.cfg.Vdisks[vdiskID] = vdiskCfg
}

// SetTemplateStorageCluster is a utility function to set a template storage cluster config, thread-safe.
func (s *StubSource) SetTemplateStorageCluster(vdiskID, clusterID string, cfg *StorageClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	if cfg != nil {
		s.setStorageCluster(clusterID, cfg)
	}

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if vdiskCfg.NBD == nil {
		vdiskCfg.NBD = &VdiskNBDConfig{
			TemplateStorageClusterID: clusterID,
		}
	} else {
		vdiskCfg.NBD.TemplateStorageClusterID = clusterID
	}

	s.cfg.Vdisks[vdiskID] = vdiskCfg
}

// SetTlogStorageCluster is a utility function to set a tlog storage cluster config, thread-safe.
func (s *StubSource) SetTlogStorageCluster(vdiskID, clusterID string, cfg *StorageClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	if cfg != nil {
		s.setStorageCluster(clusterID, cfg)
	}

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if vdiskCfg.Tlog == nil {
		vdiskCfg.Tlog = &VdiskTlogConfig{
			StorageClusterID: clusterID,
		}
	} else {
		vdiskCfg.Tlog.StorageClusterID = clusterID
	}

	s.cfg.Vdisks[vdiskID] = vdiskCfg
}

// SetSlaveStorageCluster is a utility function to set a tlog storage cluster config, thread-safe.
func (s *StubSource) SetSlaveStorageCluster(vdiskID, clusterID string, cfg *StorageClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	if cfg != nil {
		s.setStorageCluster(clusterID, cfg)
	}

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if vdiskCfg.Tlog == nil {
		vdiskCfg.Tlog = &VdiskTlogConfig{
			SlaveStorageClusterID: clusterID,
		}
	} else {
		vdiskCfg.Tlog.SlaveStorageClusterID = clusterID
	}

	if vdiskCfg.NBD == nil {
		vdiskCfg.NBD = &VdiskNBDConfig{
			SlaveStorageClusterID: clusterID,
		}
	} else {
		vdiskCfg.NBD.SlaveStorageClusterID = clusterID
	}

	s.cfg.Vdisks[vdiskID] = vdiskCfg
}

// SetTlogServerCluster is a utility function to set a tlog server cluster config, thread-safe.
func (s *StubSource) SetTlogServerCluster(vdiskID, clusterID string, cfg *TlogClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	if cfg != nil {
		s.setTlogCluster(clusterID, cfg)
	}

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if vdiskCfg.NBD == nil {
		vdiskCfg.NBD = &VdiskNBDConfig{
			TlogServerClusterID: clusterID,
		}
	} else {
		vdiskCfg.NBD.TlogServerClusterID = clusterID
	}

	s.cfg.Vdisks[vdiskID] = vdiskCfg
}

// SetStorageCluster is a utility function to set a storage cluster config, thread-safe.
func (s *StubSource) SetStorageCluster(clusterID string, cfg *StorageClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	s.setStorageCluster(clusterID, cfg)
}

// SetTlogCluster is a utility function to set a tlog cluster config, thread-safe.
func (s *StubSource) SetTlogCluster(clusterID string, cfg *TlogClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	s.setTlogCluster(clusterID, cfg)
}

// triggerReload triggers a reload of the config of this source.
func (s *StubSource) triggerReload() {
	s.watchCounterMux.RLock()
	defer s.watchCounterMux.RUnlock()
	if s.watchCounter > 0 {
		syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	}
}

func (s *StubSource) setStorageCluster(clusterID string, cfg *StorageClusterConfig) bool {
	if s.cfg == nil {
		s.cfg = &FileFormatCompleteConfig{
			StorageClusters: make(map[string]StorageClusterConfig),
		}
	} else if s.cfg.StorageClusters == nil {
		s.cfg.StorageClusters = make(map[string]StorageClusterConfig)
	}

	if cfg == nil {
		delete(s.cfg.StorageClusters, clusterID)
		return false
	}

	s.cfg.StorageClusters[clusterID] = cfg.Clone()
	return true
}

func (s *StubSource) setTlogCluster(clusterID string, cfg *TlogClusterConfig) bool {
	if s.cfg == nil {
		s.cfg = &FileFormatCompleteConfig{
			TlogClusters: make(map[string]TlogClusterConfig),
		}
	} else if s.cfg.TlogClusters == nil {
		s.cfg.TlogClusters = make(map[string]TlogClusterConfig)
	}

	if cfg == nil {
		delete(s.cfg.TlogClusters, clusterID)
		return false
	}

	s.cfg.TlogClusters[clusterID] = cfg.Clone()
	return true
}

func (s *StubSource) getVdiskCfg(vdiskID string) FileFormatVdiskConfig {
	if s.cfg == nil {
		s.cfg = &FileFormatCompleteConfig{
			Vdisks: make(map[string]FileFormatVdiskConfig),
		}
	} else if s.cfg.Vdisks == nil {
		s.cfg.Vdisks = make(map[string]FileFormatVdiskConfig)
	}

	vdiskCfg, ok := s.cfg.Vdisks[vdiskID]
	if !ok {
		vdiskCfg.BlockSize = 4096
		vdiskCfg.Size = 10
		vdiskCfg.VdiskType = VdiskTypeBoot
	}

	return vdiskCfg
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
