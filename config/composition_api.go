package config

import (
	"context"
	"errors"
	"fmt"

	"github.com/zero-os/0-Disk/log"
)

// ReadNBDStorageConfig reads, validates and returns the requested NBDStorageConfig
// from a given source, or returns an error in case something went wrong along the way.
// The NBDStorageConfig is composed out of several subconfigs,
// and thus reading this Config might require multiple roundtrips.
func ReadNBDStorageConfig(source Source, vdiskID string, staticConfig *VdiskStaticConfig) (*NBDStorageConfig, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if vdiskID == "" {
		return nil, ErrNilID
	}

	nbdConfig, err := ReadVdiskNBDConfig(source, vdiskID)
	if err != nil {
		log.Debugf("ReadNBDStorageConfig failed, invalid VdiskNBDConfig: %v", err)
		return nil, err
	}

	// Create NBD Storage config
	nbdStorageConfig := new(NBDStorageConfig)

	// Fetch Primary Storage Cluster Config
	storageClusterConfig, err := ReadStorageClusterConfig(
		source, nbdConfig.StorageClusterID)
	if err != nil {
		log.Debugf("ReadNBDStorageConfig failed, invalid StorageClusterConfig: %v", err)
		return nil, err
	}
	nbdStorageConfig.StorageCluster = *storageClusterConfig

	// if template storage ID is given, fetch it
	if nbdConfig.TemplateStorageClusterID != "" {
		// Fetch Template Storage Cluster Config
		nbdStorageConfig.TemplateStorageCluster, err = ReadStorageClusterConfig(
			source, nbdConfig.TemplateStorageClusterID)
		if err != nil {
			log.Debugf("ReadNBDStorageConfig failed, invalid TemplateStorageClusterConfig: %v", err)
			return nil, err
		}
	}

	// if slave storage ID is given, fetch it
	if nbdConfig.SlaveStorageClusterID != "" {
		// Fetch Template Storage Cluster Config
		nbdStorageConfig.SlaveStorageCluster, err = ReadStorageClusterConfig(
			source, nbdConfig.SlaveStorageClusterID)
		if err != nil {
			log.Debugf("ReadNBDStorageConfig failed, invalid SlaveStorageClusterConfig: %v", err)
			return nil, err
		}
	}

	// validate optional properties of NBD Storage Config
	// based on the storage type of this vdisk
	if staticConfig == nil {
		staticConfig, err = ReadVdiskStaticConfig(source, vdiskID)
		if err != nil {
			log.Debugf("ReadNBDStorageConfig failed, invalid VdiskStaticConfig: %v", err)
			return nil, err
		}
	}

	st := staticConfig.Type.StorageType()
	// if primary storage is required to have a metadata server, ensure it has one defined
	if st != StorageNonDeduped || (staticConfig.Type.TlogSupport() && nbdConfig.TlogServerClusterID != "") {
		err = nbdStorageConfig.StorageCluster.ValidateRequiredMetadataStorage()
		if err != nil {
			log.Debugf("ReadNBDStorageConfig failed: %v", err)
			source.MarkInvalidKey(
				Key{ID: nbdConfig.StorageClusterID, Type: KeyClusterStorage},
				vdiskID)
			return nil, NewInvalidConfigError(err)
		}

		err = nbdStorageConfig.SlaveStorageCluster.ValidateRequiredMetadataStorage()
		if err != nil {
			log.Debugf("ReadNBDStorageConfig failed: %v", err)
			source.MarkInvalidKey(
				Key{ID: nbdConfig.SlaveStorageClusterID, Type: KeyClusterStorage},
				vdiskID)
			return nil, NewInvalidConfigError(err)
		}
	}

	return nbdStorageConfig, nil
}

// ReadTlogStorageConfig reads, validates and returns the requested TlogStorageConfig
// from a given source, or returns an error in case something went wrong along the way.
// The TlogStorageConfig is composed out of several subconfigs,
// and thus reading this Config might require multiple roundtrips.
func ReadTlogStorageConfig(source Source, vdiskID string, staticConfig *VdiskStaticConfig) (*TlogStorageConfig, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if vdiskID == "" {
		return nil, ErrNilID
	}

	tlogConfig, err := ReadVdiskTlogConfig(source, vdiskID)
	if err != nil {
		log.Debugf("ReadTlogStorageConfig failed, invalid VdiskTlogConfig: %v", err)
		return nil, err
	}

	// Create TLog Storage config
	tlogStorageConfig := new(TlogStorageConfig)

	// fetch ZeroStor config
	zeroStorClusterConfig, err := ReadZeroStoreClusterConfig(source, tlogConfig.ZeroStorClusterID)
	if err != nil {
		log.Debugf("ReadZeroStoreClusterConfig failed, invalid ZeroStorClusterConfig: %v", err)
		return nil, err
	}
	tlogStorageConfig.ZeroStorCluster = *zeroStorClusterConfig

	// if slave storage ID is given, fetch it
	if tlogConfig.SlaveStorageClusterID != "" {
		tlogStorageConfig.SlaveStorageCluster, err = ReadStorageClusterConfig(
			source, tlogConfig.SlaveStorageClusterID)
		if err != nil {
			log.Debugf("ReadTlogStorageConfig failed, invalid SlaveStorageCluster: %v", err)
			return nil, err
		}

		// validate optional properties of Tlog Storage Config
		// based on the storage type of this vdisk
		if staticConfig == nil {
			staticConfig, err = ReadVdiskStaticConfig(source, vdiskID)
			if err != nil {
				log.Debugf("ReadTlogStorageConfig failed, invalid VdiskStaticConfig: %v", err)
				return nil, err
			}
		}
		err = tlogStorageConfig.ValidateOptional(staticConfig.Type.StorageType())
		if err != nil {
			log.Debugf("ReadTlogStorageConfig failed: %v", err)
			source.MarkInvalidKey(
				Key{ID: tlogConfig.SlaveStorageClusterID, Type: KeyClusterStorage},
				vdiskID)
			return nil, NewInvalidConfigError(err)
		}
	}

	return tlogStorageConfig, nil
}

// WatchTlogStorageConfig watches a given source for TlogStorageConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchTlogStorageConfig(ctx context.Context, source Source, vdiskID string) (<-chan TlogStorageConfig, error) {
	watcher, err := newTlogStorageConfigWatcher(source, vdiskID)
	if err != nil {

		return nil, err
	}

	ctx = watchContext(ctx)
	return watcher.Watch(ctx)
}

func newTlogStorageConfigWatcher(source Source, vdiskID string) (*tlogStorageConfigWatcher, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if vdiskID == "" {
		return nil, ErrNilID
	}

	return &tlogStorageConfigWatcher{
		source:  source,
		vdiskID: vdiskID,
	}, nil
}

type tlogStorageConfigWatcher struct {
	// config source (e.g. etcd)
	source Source

	// vdisk info
	vdiskID   string
	vdiskType VdiskType

	// cluster(s) info
	zeroStorClusterID, slaveClusterID string

	// local master ctx used
	ctx context.Context

	// output
	outputChan chan TlogStorageConfig

	// zerostor cluster
	zeroStorChan    <-chan ZeroStorClusterConfig
	zeroStorCancel  func()
	zeroStorCluster *ZeroStorClusterConfig

	// slave storage cluster
	slaveChan    <-chan StorageClusterConfig
	slaveCancel  func()
	slaveCluster *StorageClusterConfig
}

func (w *tlogStorageConfigWatcher) Watch(ctx context.Context) (<-chan TlogStorageConfig, error) {
	err := w.fetchVdiskType()
	if err != nil {
		return nil, err
	}

	var cancelWatch func()
	w.ctx, cancelWatch = context.WithCancel(ctx)

	clusterChan, err := WatchVdiskTlogConfig(ctx, w.source, w.vdiskID)
	if err != nil {
		cancelWatch()
		log.Debugf("TlogStorageConfigWatcher (%s) failed, invalid VdiskTlogConfig: %v", w.vdiskID, err)
		return nil, err
	}
	select {
	case <-ctx.Done():
		cancelWatch()
		return nil, fmt.Errorf(
			"TlogStorageConfigWatcher (%s) failed: %v", w.vdiskID, ErrContextDone)

	case clusterInfo := <-clusterChan:
		_, err = w.applyClusterInfo(clusterInfo)
		if err != nil {
			cancelWatch()
			log.Debugf(
				"TlogStorageConfigWatcher (%s) failed, err with initial config: %v",
				w.vdiskID, err)
			return nil, err
		}
	}

	// create output channel
	w.outputChan = make(chan TlogStorageConfig, 1)
	// send initial config
	err = w.sendOutput()
	if err != nil {
		cancelWatch()
		log.Debugf("TlogStorageConfigWatcher failed, err with initial config: %v", err)
		return nil, err
	}

	go func() {
		defer cancelWatch()
		defer close(w.outputChan)

		for {
			select {
			case <-ctx.Done():
				return // done

			// update cluster config
			case clusterInfo, open := <-clusterChan:
				if !open {
					log.Debugf(
						"close TlogStorageConfigWatcher (%s) because of closed cluster chan", w.vdiskID)
					return
				}
				log.Debugf("TlogStorageConfigWatcher (%s) receives cluster info", w.vdiskID)
				changed, err := w.applyClusterInfo(clusterInfo)
				if err != nil {
					log.Errorf(
						"TlogStorageConfigWatcher failed, err with update config: %v", err)
					changed = false
				}
				if !changed {
					continue
				}
			// update zeroStor cluster
			case cluster, open := <-w.zeroStorChan:
				if !open {
					log.Debugf(
						"close TlogStorageConfigWatcher (%s) because of closed 0-stor chan", w.vdiskID)
					return
				}
				log.Debugf("TlogStorageConfigWatcher (%s) receives 0-stor cluster", w.vdiskID)
				if w.zeroStorCluster.Equal(&cluster) {
					log.Debugf(
						"TlogStorageConfigWatcher (%s) received nop-update for 0-stor cluster (%s)",
						w.vdiskID, w.zeroStorClusterID)
					continue
				}
				w.zeroStorCluster = &cluster

			// update slave cluster
			case cluster, open := <-w.slaveChan:
				log.Debugf("TlogStorageConfigWatcher (%s) receives slave info", w.vdiskID)
				if !open {
					log.Debugf(
						"close TlogStorageConfigWatcher (%s) because of closed slave chan", w.vdiskID)
					return
				}
				if w.slaveCluster.Equal(&cluster) {
					log.Debugf(
						"TlogStorageConfigWatcher (%s) received nop-update for slave cluster (%s)",
						w.vdiskID, w.slaveClusterID)
					continue
				}
				err := cluster.ValidateStorageType(w.vdiskType.StorageType())
				if err != nil {
					log.Errorf(
						"TlogStorageConfigWatcher failed, err with slave cluster config: %v", err)
					w.source.MarkInvalidKey(Key{ID: w.slaveClusterID, Type: KeyClusterStorage}, w.vdiskID)
					continue
				}
				w.slaveCluster = &cluster
			}

			// send new output, as a cluster has been updated
			if err := w.sendOutput(); err != nil {
				log.Errorf(
					"TlogStorageConfigWatcher failed to send value: %v", err)
			}
		}
	}()

	return w.outputChan, nil
}

func (w *tlogStorageConfigWatcher) fetchVdiskType() error {
	staticConfig, err := ReadVdiskStaticConfig(w.source, w.vdiskID)
	if err != nil {
		log.Debugf("TlogStorageConfigWatcher failed, invalid VdiskStaticConfig: %v", err)
		return err
	}

	w.vdiskType = staticConfig.Type
	return nil
}

func (w *tlogStorageConfigWatcher) sendOutput() error {
	if w.zeroStorCluster == nil {
		return errors.New("0-stor storage is nil, while it is required")
	}

	cfg := TlogStorageConfig{ZeroStorCluster: w.zeroStorCluster.Clone()}
	if w.slaveCluster != nil {
		slaveCluster := w.slaveCluster.Clone()
		cfg.SlaveStorageCluster = &slaveCluster
	}

	err := cfg.Validate(w.vdiskType.StorageType())
	if err != nil {
		err = NewInvalidConfigError(err)
		log.Errorf("tlogStorageConfigWatcher: has unexpected invalid output to sent: %v", err)
		return err
	}

	select {
	case w.outputChan <- cfg:
		return nil // ok
	case <-w.ctx.Done():
		return ErrContextDone
	}
}

func (w *tlogStorageConfigWatcher) applyClusterInfo(info VdiskTlogConfig) (bool, error) {
	log.Debugf(
		"TlogStorageConfigWatcher (%s) loading 0-stor storage cluster: %s",
		w.vdiskID, info.ZeroStorClusterID)
	zeroStorChanged, err := w.loadZeroStorChan(info.ZeroStorClusterID)
	if err != nil {
		return false, err
	}

	log.Debugf(
		"TlogStorageConfigWatcher (%s) loading slave storage cluster: %s",
		w.vdiskID, info.SlaveStorageClusterID)
	slaveChanged, err := w.loadSlaveChan(info.SlaveStorageClusterID)
	if err != nil {
		return zeroStorChanged, nil
	}

	changed := zeroStorChanged || slaveChanged
	return changed, nil
}

func (w *tlogStorageConfigWatcher) loadZeroStorChan(clusterID string) (bool, error) {
	if w.zeroStorClusterID == clusterID {
		log.Debugf(
			"TlogStorageConfigWatcher (%s) already watches 0-stor cluster: %s",
			w.vdiskID, clusterID)
		return false, nil // nothing to do
	}

	if clusterID == "" {
		log.Errorf(
			"TlogStorageConfigWatcher (%s) received request to load 0-stor cluster using a nil-id",
			w.vdiskID)
		return false, ErrNilID
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchZeroStorClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		log.Debugf("TlogStorageConfigWatcher failed, invalid ZeroStorClusterConfig: %v", err)
		return false, err
	}

	// read initial value
	var config ZeroStorClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-ch:
	}

	// 0-stor cluster has been switched successfully
	w.zeroStorChan = ch
	if w.zeroStorCancel != nil {
		w.zeroStorCancel()
	}
	w.zeroStorCancel = cancel
	w.zeroStorCluster = &config
	w.zeroStorClusterID = clusterID

	return true, nil
}

func (w *tlogStorageConfigWatcher) loadSlaveChan(clusterID string) (bool, error) {
	if w.slaveClusterID == clusterID {
		log.Debugf(
			"TlogStorageConfigWatcher (%s) already watches slave cluster: %s",
			w.vdiskID, clusterID)
		return false, nil // nothing to do
	}

	if clusterID == "" {
		log.Infof(
			"deleting previously used slave cluster %s for vdisk %s",
			clusterID, w.vdiskID)
		// meaning we want to delete slave cluster
		w.slaveClusterID = ""
		w.slaveChan = nil
		w.slaveCancel()
		w.slaveCancel = nil
		w.slaveCluster = nil // make sure storage is set to nil
		return true, nil
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		log.Debugf("TlogStorageConfigWatcher failed, invalid SlaveStorageClusterConfig: %v", err)
		return false, err
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-ch:
	}

	// ensure slave cluster is valid for this storage Type
	err = config.ValidateStorageType(w.vdiskType.StorageType())
	if err != nil {
		cancel()
		w.source.MarkInvalidKey(Key{ID: clusterID, Type: KeyClusterStorage}, w.vdiskID)
		return false, NewInvalidConfigError(err)
	}

	// slave cluster has been switched successfully
	w.slaveChan = ch
	if w.slaveCancel != nil {
		w.slaveCancel()
	}
	w.slaveCancel = cancel
	w.slaveCluster = &config
	w.slaveClusterID = clusterID
	return true, nil
}

// WatchNBDStorageConfig watches a given source for NBDStorageConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchNBDStorageConfig(ctx context.Context, source Source, vdiskID string) (<-chan NBDStorageConfig, error) {
	watcher, err := newNBDStorageConfigWatcher(source, vdiskID)
	if err != nil {
		return nil, err
	}

	ctx = watchContext(ctx)
	return watcher.Watch(ctx)
}

func newNBDStorageConfigWatcher(source Source, vdiskID string) (*nbdStorageConfigWatcher, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if vdiskID == "" {
		return nil, ErrNilID
	}

	return &nbdStorageConfigWatcher{
		source:  source,
		vdiskID: vdiskID,
	}, nil
}

type nbdStorageConfigWatcher struct {
	// config source (e.g. etcd)
	source Source

	// vdisk info
	vdiskID   string
	vdiskType VdiskType

	// cluster(s) info
	primaryClusterID, templateClusterID, slaveClusterID string
	// only used for validation
	tlogClusterID string

	// local master ctx used
	ctx context.Context

	// output
	outputChan chan NBDStorageConfig

	// primary storage cluster
	primaryChan    <-chan StorageClusterConfig
	primaryCancel  func()
	primaryCluster *StorageClusterConfig

	// template storage cluster
	templateChan    <-chan StorageClusterConfig
	templateCancel  func()
	templateCluster *StorageClusterConfig

	// slave storage cluster
	slaveChan    <-chan StorageClusterConfig
	slaveCancel  func()
	slaveCluster *StorageClusterConfig
}

func (w *nbdStorageConfigWatcher) Watch(ctx context.Context) (<-chan NBDStorageConfig, error) {
	err := w.fetchVdiskType()
	if err != nil {
		return nil, err
	}

	var cancelWatch func()
	w.ctx, cancelWatch = context.WithCancel(ctx)

	clusterChan, err := WatchVdiskNBDConfig(ctx, w.source, w.vdiskID)
	if err != nil {
		cancelWatch()
		log.Errorf("nbdStorageConfigWatcher failed, invalid VdiskNBDConfig: %v", err)
		return nil, err
	}
	select {
	case <-ctx.Done():
		cancelWatch()
		log.Debugf("nbdStorageConfigWatcher failed: %v", ErrContextDone)
		return nil, ErrContextDone

	case clusterInfo := <-clusterChan:
		_, err = w.applyClusterInfo(clusterInfo)
		if err != nil {
			cancelWatch()
			log.Errorf("nbdStorageConfigWatcher failed, err with initial config: %v", err)
			return nil, err
		}
	}

	// create output channel
	w.outputChan = make(chan NBDStorageConfig, 1)
	// send initial config
	err = w.sendOutput()
	if err != nil {
		cancelWatch()
		log.Errorf("nbdStorageConfigWatcher failed, err with initial config: %v", err)
		return nil, err
	}

	go func() {
		defer cancelWatch()
		defer close(w.outputChan)

		for {
			select {
			case <-ctx.Done():
				return // done

			// update cluster config
			case clusterInfo, open := <-clusterChan:
				if !open {
					log.Debugf(
						"close nbdStorageConfigWatcher (%s) because of closed cluster chan", w.vdiskID)
					return
				}
				log.Debugf("nbdStorageConfigWatcher (%s) receives cluster info", w.vdiskID)
				changed, err := w.applyClusterInfo(clusterInfo)
				if err != nil {
					log.Errorf(
						"nbdStorageConfigWatcher failed, err with update config: %v", err)
					changed = false
				}
				if !changed {
					continue
				}

			// update primary cluster
			case cluster, open := <-w.primaryChan:
				if !open {
					log.Debugf(
						"close nbdStorageConfigWatcher (%s) because of closed primary chan", w.vdiskID)
					return
				}
				log.Debugf("nbdStorageConfigWatcher (%s) receives primary cluster", w.vdiskID)
				if w.primaryCluster.Equal(&cluster) {
					log.Debugf(
						"nbdStorageConfigWatcher (%s) received nop-update for primary cluster (%s)",
						w.vdiskID, w.primaryClusterID)
					continue
				}
				err := cluster.ValidateStorageType(w.vdiskType.StorageType())
				if err != nil {
					log.Errorf(
						"nbdStorageConfigWatcher failed, err with primary cluster config: %v", err)
					w.source.MarkInvalidKey(Key{ID: w.primaryClusterID, Type: KeyClusterStorage}, w.vdiskID)
					continue
				}
				w.primaryCluster = &cluster

			// update template cluster
			case cluster, open := <-w.templateChan:
				if !open {
					log.Debugf(
						"close nbdStorageConfigWatcher (%s) because of closed template chan", w.vdiskID)
					return
				}
				log.Debugf("nbdStorageConfigWatcher (%s) receives template cluster", w.vdiskID)
				if w.templateCluster.Equal(&cluster) {
					log.Debugf(
						"nbdStorageConfigWatcher (%s) received nop-update for template cluster (%s)",
						w.vdiskID, w.templateClusterID)
					continue
				}
				w.templateCluster = &cluster

			// update slave cluster
			case cluster, open := <-w.slaveChan:
				if !open {
					log.Debugf(
						"close nbdStorageConfigWatcher (%s) because of closed slave chan", w.vdiskID)
					return
				}
				log.Debugf("nbdStorageConfigWatcher (%s) receives slave cluster", w.vdiskID)
				if w.slaveCluster.Equal(&cluster) {
					log.Debugf(
						"nbdStorageConfigWatcher (%s) received nop-update for slave cluster (%s)",
						w.vdiskID, w.slaveClusterID)
					continue
				}
				err := cluster.ValidateStorageType(w.vdiskType.StorageType())
				if err != nil {
					log.Errorf(
						"nbdStorageConfigWatcher failed, err with slave cluster config: %v", err)
					w.source.MarkInvalidKey(Key{ID: w.slaveClusterID, Type: KeyClusterStorage}, w.vdiskID)
					continue
				}
				w.slaveCluster = &cluster
			}

			// send new output, as a cluster has been updated
			if err := w.sendOutput(); err != nil {
				log.Errorf("nbdStorageConfigWatcher failed to send value: %v", err)
			}
		}
	}()

	return w.outputChan, nil
}

func (w *nbdStorageConfigWatcher) fetchVdiskType() error {
	staticConfig, err := ReadVdiskStaticConfig(w.source, w.vdiskID)
	if err != nil {
		log.Debugf("nbdStorageConfigWatcher failed, invalid VdiskStaticConfig: %v", err)
		return err
	}

	w.vdiskType = staticConfig.Type
	return nil
}

func (w *nbdStorageConfigWatcher) sendOutput() error {
	if w.primaryCluster == nil {
		return errors.New("primary storage is nil, while it is required")
	}

	cfg := NBDStorageConfig{StorageCluster: w.primaryCluster.Clone()}
	if w.templateCluster != nil {
		templateCluster := w.templateCluster.Clone()
		cfg.TemplateStorageCluster = &templateCluster
	}
	if w.slaveCluster != nil {
		slaveCluster := w.slaveCluster.Clone()
		cfg.SlaveStorageCluster = &slaveCluster
	}

	err := cfg.Validate(w.vdiskType.StorageType())
	if err != nil {
		return err
	}

	select {
	case w.outputChan <- cfg:
		return nil // ok
	case <-w.ctx.Done():
		return ErrContextDone
	}
}

func (w *nbdStorageConfigWatcher) applyClusterInfo(info VdiskNBDConfig) (bool, error) {
	metadataRequired := w.vdiskType.StorageType() != StorageNonDeduped || // is required if storage isn't nonDeduped ...
		(w.vdiskType.TlogSupport() && info.TlogServerClusterID != "") // ... and/or if vdisk has tlogsupport and makes use of it

	var tlogClusterIDChanged bool
	if w.tlogClusterID != info.TlogServerClusterID {
		tlogClusterIDChanged = true
		w.tlogClusterID = info.TlogServerClusterID
	}

	log.Debugf(
		"nbdStorageConfigWatcher (%s) loading primary storage cluster: %s",
		w.vdiskID, info.StorageClusterID)
	primaryChanged, err := w.loadPrimaryChan(info.StorageClusterID, metadataRequired, tlogClusterIDChanged)
	if err != nil {
		return false, err
	}

	log.Debugf(
		"nbdStorageConfigWatcher (%s) loading template storage cluster: %s",
		w.vdiskID, info.TemplateStorageClusterID)
	templateClusterChanged, err := w.loadTemplateChan(info.TemplateStorageClusterID)
	if err != nil {
		log.Errorf("error occured in nbdStorageConfigWatcher, template is invalid: %v", err)
		return primaryChanged, nil
	}

	log.Debugf(
		"nbdStorageConfigWatcher (%s) loading slave storage cluster: %s",
		w.vdiskID, info.SlaveStorageClusterID)
	slaveClusterChanged, err := w.loadSlaveChan(info.SlaveStorageClusterID, metadataRequired, tlogClusterIDChanged)
	if err != nil {
		log.Errorf("error occured in nbdStorageConfigWatcher, slave is invalid: %v", err)
		return primaryChanged || templateClusterChanged, nil
	}

	changed := primaryChanged || templateClusterChanged || slaveClusterChanged
	return changed, nil
}

func (w *nbdStorageConfigWatcher) loadPrimaryChan(clusterID string, metadataRequired, tlogClusterIDChanged bool) (bool, error) {
	if w.primaryClusterID == clusterID {
		if tlogClusterIDChanged && metadataRequired {
			// if tlog cluster changed and
			// ensure that the primary cluster still validates
			err := w.primaryCluster.ValidateRequiredMetadataStorage()
			if err != nil {
				w.source.MarkInvalidKey(Key{ID: w.primaryClusterID, Type: KeyClusterStorage}, w.vdiskID)
				return false, NewInvalidConfigError(err)
			}
		}

		log.Debugf(
			"nbdStorageConfigWatcher (%s) already watches primary cluster: %s",
			w.vdiskID, clusterID)
		return false, nil // nothing to do
	}
	if clusterID == "" {
		log.Errorf(
			"nbdStorageConfigWatcher (%s) received request to load primary cluster using a nil-id",
			w.vdiskID)
		return false, ErrNilID
	}

	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		log.Debugf("nbdStorageConfigWatcher failed, invalid PrimaryStorageClusterConfig: %v", err)
		return false, err
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-ch:
	}

	// validate if metadata is defined if required
	if metadataRequired {
		err = config.ValidateRequiredMetadataStorage()
		if err != nil {
			cancel()
			w.source.MarkInvalidKey(Key{ID: clusterID, Type: KeyClusterStorage}, w.vdiskID)
			return false, NewInvalidConfigError(err)
		}
	}

	// primary cluster has been switched successfully
	w.primaryChan = ch
	if w.primaryCancel != nil {
		w.primaryCancel()
	}
	w.primaryCancel = cancel
	w.primaryCluster = &config
	w.primaryClusterID = clusterID
	return true, nil
}

func (w *nbdStorageConfigWatcher) loadTemplateChan(clusterID string) (bool, error) {
	if w.templateClusterID == clusterID {
		log.Debugf(
			"nbdStorageConfigWatcher (%s) already watches template cluster: %s",
			w.vdiskID, clusterID)
		return false, nil // nothing to do
	}

	if clusterID == "" {
		log.Infof(
			"deleting previously used template cluster %s for vdisk %s",
			clusterID, w.vdiskID)
		// meaning we want to delete template cluster
		w.templateClusterID = ""
		w.templateCancel()
		w.templateChan = nil
		w.templateCancel = nil
		w.templateCluster = nil // make sure storage is set to nil
		return true, nil
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		log.Debugf("nbdStorageConfigWatcher failed, invalid TemplateStorageClusterConfig: %v", err)
		return false, err
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-ch:
	}

	// validate template storage cluster
	err = config.ValidateStorageType(w.vdiskType.StorageType())
	if err != nil {
		cancel()
		w.source.MarkInvalidKey(Key{ID: clusterID, Type: KeyClusterStorage}, w.vdiskID)
		return false, NewInvalidConfigError(err)
	}

	// template cluster has been switched successfully
	w.templateChan = ch
	if w.templateCancel != nil {
		w.templateCancel()
	}
	w.templateCancel = cancel
	w.templateCluster = &config
	w.templateClusterID = clusterID
	return true, nil
}

func (w *nbdStorageConfigWatcher) loadSlaveChan(clusterID string, metadataRequired, tlogClusterIDChanged bool) (bool, error) {
	if w.slaveClusterID == clusterID {
		if tlogClusterIDChanged && w.slaveClusterID != "" && metadataRequired {
			// if tlog cluster changed and slave cluster was defined previously,
			// ensure that the slave cluster still validates
			err := w.slaveCluster.ValidateRequiredMetadataStorage()
			if err != nil {
				w.source.MarkInvalidKey(Key{ID: w.slaveClusterID, Type: KeyClusterStorage}, w.vdiskID)
				return false, NewInvalidConfigError(err)
			}
		}

		log.Debugf(
			"nbdStorageConfigWatcher (%s) already watches slave cluster: %s",
			w.vdiskID, clusterID)
		return false, nil // nothing to do
	}

	if clusterID == "" {
		log.Infof(
			"deleting previously used slave cluster %s for vdisk %s",
			clusterID, w.vdiskID)
		// meaning we want to delete slave cluster
		w.slaveClusterID = ""
		w.slaveCancel()
		w.slaveChan = nil
		w.slaveCancel = nil
		w.slaveCluster = nil // make sure storage is set to nil
		return true, nil
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		log.Debugf("nbdStorageConfigWatcher failed, invalid SlaveStorageClusterConfig: %v", err)
		return false, err
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-ch:
	}

	// validate if metadata is defined if required
	if metadataRequired {
		err = config.ValidateRequiredMetadataStorage()
		if err != nil {
			cancel()
			w.source.MarkInvalidKey(Key{ID: clusterID, Type: KeyClusterStorage}, w.vdiskID)
			return false, NewInvalidConfigError(err)
		}
	}

	// slave cluster has been switched successfully
	w.slaveChan = ch
	if w.slaveCancel != nil {
		w.slaveCancel()
	}
	w.slaveCancel = cancel
	w.slaveCluster = &config
	w.slaveClusterID = clusterID
	return true, nil
}
