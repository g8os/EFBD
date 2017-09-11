package config

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/zero-os/0-Disk/log"
)

// NewStubSource create a new stub source, for testing purposes
func NewStubSource() *StubSource {
	source := new(StubSource)
	source.fileSource.path = "/tests/in/memory"
	source.fileSource.reader = source.readConfig
	source.subscribers = make(map[chan []byte]Key)

	return source
}

// StubSource is a modified file source using an internal stored config in memory
// used for testing purposes only.
type StubSource struct {
	fileSource

	cfg *FileFormatCompleteConfig
	mux sync.Mutex

	subscribers map[chan []byte]Key
	submux      sync.Mutex

	invalidConfigSender chan Key
}

// Watch implements Source.Watch
func (s *StubSource) Watch(ctx context.Context, key Key) (<-chan []byte, error) {
	output := make(chan []byte, 1)
	s.submux.Lock()
	s.subscribers[output] = key
	s.submux.Unlock()

	go func() {
		<-ctx.Done()
		s.submux.Lock()
		delete(s.subscribers, output)
		s.submux.Unlock()
	}()

	return output, nil
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

// Type implements Source.Type
func (s *StubSource) Type() string {
	return "stub"
}

// InvalidKey can be used to get a channel to wait for
// an incoming invalid key.
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

// SetTlogZeroStorCluster is a utility function to set a tlog storage zerostor config, thread-safe.
func (s *StubSource) SetTlogZeroStorCluster(vdiskID, clusterID string, cfg *ZeroStorClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	if cfg != nil {
		s.setZeroStorCluster(clusterID, cfg)
	}

	vdiskCfg := s.getVdiskCfg(vdiskID)

	if vdiskCfg.Tlog == nil {
		vdiskCfg.Tlog = &VdiskTlogConfig{
			ZeroStorClusterID: clusterID,
		}
	} else {
		vdiskCfg.Tlog.ZeroStorClusterID = clusterID
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

// SetZeroStorCluster is a utility function to set a 0-stor cluster config, thread-safe.
func (s *StubSource) SetZeroStorCluster(clusterID string, cfg *ZeroStorClusterConfig) {
	s.mux.Lock()
	defer s.mux.Unlock()
	defer s.triggerReload()

	s.setZeroStorCluster(clusterID, cfg)
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
	s.submux.Lock()
	defer s.submux.Unlock()

	var wg sync.WaitGroup

	for ch, key := range s.subscribers {
		wg.Add(1)

		go func(ch chan []byte, key Key) {
			defer wg.Done()

			// we ignore any error cases which aren't
			// due to the source being unavailable,
			// as we want to be able to mark any invalid key
			// for those other error cases (e.g. invalid key, invalid id, ...)
			bytes, err := s.Get(key)
			if err == ErrSourceUnavailable {
				log.Errorf(
					"getting key %v failed, due to the source being unavailable",
					key)
				return
			}

			select {
			case ch <- bytes:
				// ok (might send nil)
			case <-time.After(time.Second):
				log.Errorf("sending config for %v has timed out", key)
			}
		}(ch, key)
	}

	wg.Wait()
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

func (s *StubSource) setZeroStorCluster(clusterID string, cfg *ZeroStorClusterConfig) bool {
	if s.cfg == nil {
		s.cfg = &FileFormatCompleteConfig{
			ZeroStorClusters: make(map[string]ZeroStorClusterConfig),
		}
	} else if s.cfg.ZeroStorClusters == nil {
		s.cfg.ZeroStorClusters = make(map[string]ZeroStorClusterConfig)
	}

	if cfg == nil {
		delete(s.cfg.ZeroStorClusters, clusterID)
		return false
	}

	s.cfg.ZeroStorClusters[clusterID] = cfg.Clone()
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
	if s.cfg == nil {
		return nil, errors.New("stub: no test config defined")
	}

	return serializeConfigReply(s.cfg, nil)
}
