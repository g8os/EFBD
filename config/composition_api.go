package config

import (
	"context"
	"errors"
	"fmt"

	"github.com/siddontang/go/log"
)

// ReadNBDStorageConfig reads, validates and returns the requested NBDStorageConfig
// from a given source, or returns an error in case something went wrong along the way.
// The NBDStorageConfig is composed out of several subconfigs,
// and thus reading this Config might require multiple roundtrips.
func ReadNBDStorageConfig(source Source, vdiskID string) (*NBDStorageConfig, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if vdiskID == "" {
		return nil, ErrNilID
	}

	nbdConfig, err := ReadVdiskNBDConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadNBDStorageConfig failed, invalid VdiskNBDConfig: %v", err)
	}

	// Create NBD Storage config
	nbdStorageConfig := new(NBDStorageConfig)

	// Fetch Primary Storage Cluster Config
	storageClusterConfig, err := ReadStorageClusterConfig(
		source, nbdConfig.StorageClusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadNBDStorageConfig failed, invalid StorageClusterConfig: %v", err)
	}
	nbdStorageConfig.StorageCluster = *storageClusterConfig

	// if template storage ID is given, fetch it
	if nbdConfig.TemplateStorageClusterID != "" {
		nbdStorageConfig.TemplateVdiskID = nbdConfig.TemplateVdiskID

		// Fetch Template Storage Cluster Config
		nbdStorageConfig.TemplateStorageCluster, err = ReadStorageClusterConfig(
			source, nbdConfig.TemplateStorageClusterID)
		if err != nil {
			return nil, fmt.Errorf(
				"ReadNBDStorageConfig failed, invalid TemplateStorageClusterConfig: %v", err)
		}
	}

	// validate optional properties of NBD Storage Config
	// based on the storage type of this vdisk
	staticConfig, err := ReadVdiskStaticConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadNBDStorageConfig failed, invalid VdiskStaticConfig: %v", err)
	}
	err = nbdStorageConfig.ValidateOptional(staticConfig.Type.StorageType())
	if err != nil {
		return nil, fmt.Errorf("ReadNBDStorageConfig failed: %v", err)
	}

	return nbdStorageConfig, nil
}

// ReadTlogStorageConfig reads, validates and returns the requested TlogStorageConfig
// from a given source, or returns an error in case something went wrong along the way.
// The TlogStorageConfig is composed out of several subconfigs,
// and thus reading this Config might require multiple roundtrips.
func ReadTlogStorageConfig(source Source, vdiskID string) (*TlogStorageConfig, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if vdiskID == "" {
		return nil, ErrNilID
	}

	tlogConfig, err := ReadVdiskTlogConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadTlogStorageConfig failed, invalid VdiskTlogConfig: %v", err)
	}

	// Create NBD Storage config
	tlogStorageConfig := new(TlogStorageConfig)

	// Fetch Tlog Storage Cluster Config
	storageClusterConfig, err := ReadStorageClusterConfig(
		source, tlogConfig.StorageClusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"ReadTlogStorageConfig failed, invalid StorageClusterConfig: %v", err)
	}
	tlogStorageConfig.StorageCluster = *storageClusterConfig

	// if slave storage ID is given, fetch it
	if tlogConfig.SlaveStorageClusterID != "" {
		tlogStorageConfig.SlaveStorageCluster, err = ReadStorageClusterConfig(
			source, tlogConfig.SlaveStorageClusterID)
		if err != nil {
			return nil, fmt.Errorf(
				"ReadTlogStorageConfig failed, invalid SlaveStorageCluster: %v", err)
		}

		// validate optional properties of NBD Storage Config
		// based on the storage type of this vdisk
		staticConfig, err := ReadVdiskStaticConfig(source, vdiskID)
		if err != nil {
			return nil, fmt.Errorf(
				"ReadTlogStorageConfig failed, invalid VdiskStaticConfig: %v", err)
		}
		err = tlogStorageConfig.ValidateOptional(staticConfig.Type.StorageType())
		if err != nil {
			return nil, fmt.Errorf("ReadTlogStorageConfig failed: %v", err)
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
	tlogClusterID, slaveClusterID string

	// local master ctx used
	ctx context.Context

	// output
	outputChan chan TlogStorageConfig

	// tlog storage cluster
	tlogChan    <-chan StorageClusterConfig
	tlogCancel  func()
	tlogStorage *StorageClusterConfig

	// slave storage cluster
	slaveChan    <-chan StorageClusterConfig
	slaveCancel  func()
	slaveStorage *StorageClusterConfig
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
		return nil, fmt.Errorf(
			"TlogStorageConfigWatcher failed, invalid VdiskTlogConfig: %v", err)
	}
	select {
	case <-ctx.Done():
		cancelWatch()
		return nil, fmt.Errorf(
			"TlogStorageConfigWatcher failed: %v", ErrContextDone)

	case clusterInfo := <-clusterChan:
		_, err = w.applyClusterInfo(clusterInfo)
		if err != nil {
			cancelWatch()
			return nil, fmt.Errorf(
				"TlogStorageConfigWatcher failed, err with initial config: %v", err)
		}
	}

	// create output channel
	w.outputChan = make(chan TlogStorageConfig, 1)
	// send initial config
	err = w.sendOutput()
	if err != nil {
		cancelWatch()
		return nil, fmt.Errorf(
			"TlogStorageConfigWatcher failed, err with initial config: %v", err)
	}

	go func() {
		defer cancelWatch()
		defer close(w.outputChan)

		for {
			select {
			case <-ctx.Done():
				return // done

			// update tlog cluster
			case cluster := <-w.tlogChan:
				w.tlogStorage = &cluster

			// update slave cluster
			case cluster := <-w.slaveChan:
				w.slaveStorage = &cluster

			// update cluster config
			case clusterInfo := <-clusterChan:
				changed, err := w.applyClusterInfo(clusterInfo)
				if err != nil {
					// TODO: notify 0-orchestrator
					log.Errorf(
						"TlogStorageConfigWatcher failed, err with update config: %v", err)
				}
				if !changed {
					continue
				}
			}

			// send new output, as a cluster has been updated
			if err := w.sendOutput(); err != nil {
				log.Errorf(
					"TlogStorageConfigWatcher failed to send value: %v", err)
				return
			}
		}
	}()

	return w.outputChan, nil
}

func (w *tlogStorageConfigWatcher) fetchVdiskType() error {
	staticConfig, err := ReadVdiskStaticConfig(w.source, w.vdiskID)
	if err != nil {
		return fmt.Errorf(
			"TlogStorageConfigWatcher failed, invalid VdiskStaticConfig: %v", err)
	}

	w.vdiskType = staticConfig.Type
	return nil
}

func (w *tlogStorageConfigWatcher) sendOutput() error {
	if w.tlogStorage == nil {
		return errors.New("tlog storage is nil, while it is required")
	}

	cfg := TlogStorageConfig{StorageCluster: w.tlogStorage.Clone()}
	if w.slaveStorage != nil {
		slaveCluster := w.slaveStorage.Clone()
		cfg.SlaveStorageCluster = &slaveCluster
	}

	select {
	case w.outputChan <- cfg:
		return nil // ok
	case <-w.ctx.Done():
		return ErrContextDone
	}
}

func (w *tlogStorageConfigWatcher) applyClusterInfo(info VdiskTlogConfig) (bool, error) {
	tlogChanged, err := w.loadTlogChan(info.StorageClusterID)
	if err != nil {
		return false, err
	}

	slaveChanged, err := w.loadSlaveChan(info.SlaveStorageClusterID)
	if err != nil {
		log.Errorf("error occured in TlogStorageConfigWatcher, slave is invalid: %v", err)
		return tlogChanged, nil
	}

	changed := tlogChanged || slaveChanged
	return changed, nil
}

func (w *tlogStorageConfigWatcher) loadTlogChan(clusterID string) (bool, error) {
	if w.tlogClusterID == clusterID {
		return false, nil // nothing to do
	}
	if clusterID == "" {
		return false, ErrNilID
	}

	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		return false, fmt.Errorf(
			"TlogStorageConfigWatcher failed, invalid TLogStorageClusterConfig: %v", err)
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-w.tlogChan:
	}

	// tlog cluster has been switched successfully
	w.tlogChan = ch
	w.tlogCancel = cancel
	w.tlogStorage = &config
	w.tlogClusterID = clusterID
	return true, nil
}

func (w *tlogStorageConfigWatcher) loadSlaveChan(clusterID string) (bool, error) {
	if w.slaveClusterID == clusterID {
		return false, nil // nothing to do
	}

	if clusterID == "" {
		log.Infof(
			"deleting previously used slave cluster %s for vdisk %s",
			clusterID, w.vdiskID)
		// meaning we want to delete slave cluster
		w.slaveClusterID = ""
		w.slaveCancel()
		w.slaveCancel = nil
		w.slaveStorage = nil // make sure storage is set to nil
		return true, nil
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		return false, fmt.Errorf(
			"TlogStorageConfigWatcher failed, invalid TLogStorageClusterConfig: %v", err)
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-w.slaveChan:
	}

	// slave cluster has been switched successfully
	w.slaveChan = ch
	w.slaveCancel = cancel
	w.slaveStorage = &config
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
	primaryClusterID, templateClusterID string

	// local master ctx used
	ctx context.Context

	// output
	outputChan chan NBDStorageConfig

	// primary storage cluster
	primaryChan    <-chan StorageClusterConfig
	primaryCancel  func()
	primaryStorage *StorageClusterConfig

	// template storage cluster
	templateChan    <-chan StorageClusterConfig
	templateCancel  func()
	templateStorage *StorageClusterConfig
	templateVdiskID string
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
		return nil, fmt.Errorf(
			"nbdStorageConfigWatcher failed, invalid VdiskNBDConfig: %v", err)
	}
	select {
	case <-ctx.Done():
		cancelWatch()
		return nil, fmt.Errorf(
			"nbdStorageConfigWatcher failed: %v", ErrContextDone)

	case clusterInfo := <-clusterChan:
		_, err = w.applyClusterInfo(clusterInfo)
		if err != nil {
			cancelWatch()
			return nil, fmt.Errorf(
				"nbdStorageConfigWatcher failed, err with initial config: %v", err)
		}
	}

	// create output channel
	w.outputChan = make(chan NBDStorageConfig, 1)
	// send initial config
	err = w.sendOutput()
	if err != nil {
		cancelWatch()
		return nil, fmt.Errorf(
			"nbdStorageConfigWatcher failed, err with initial config: %v", err)
	}

	go func() {
		defer cancelWatch()
		defer close(w.outputChan)

		for {
			select {
			case <-ctx.Done():
				return // done

			// update primary cluster
			case cluster := <-w.primaryChan:
				w.primaryStorage = &cluster

			// update template cluster
			case cluster := <-w.templateChan:
				w.templateStorage = &cluster

			// update cluster config
			case clusterInfo := <-clusterChan:
				changed, err := w.applyClusterInfo(clusterInfo)
				if err != nil {
					// TODO: notify 0-orchestrator
					log.Errorf(
						"nbdStorageConfigWatcher failed, err with update config: %v", err)
				}
				if !changed {
					continue
				}
			}

			// send new output, as a cluster has been updated
			if err := w.sendOutput(); err != nil {
				log.Errorf(
					"nbdStorageConfigWatcher failed to send value: %v", err)
				return
			}
		}
	}()

	return w.outputChan, nil
}

func (w *nbdStorageConfigWatcher) fetchVdiskType() error {
	staticConfig, err := ReadVdiskStaticConfig(w.source, w.vdiskID)
	if err != nil {
		return fmt.Errorf(
			"nbdStorageConfigWatcher failed, invalid VdiskStaticConfig: %v", err)
	}

	w.vdiskType = staticConfig.Type
	return nil
}

func (w *nbdStorageConfigWatcher) sendOutput() error {
	if w.primaryStorage == nil {
		return errors.New("primary storage is nil, while it is required")
	}

	cfg := NBDStorageConfig{StorageCluster: w.primaryStorage.Clone()}
	if w.templateStorage != nil {
		templateCluster := w.templateStorage.Clone()
		cfg.TemplateStorageCluster = &templateCluster
		cfg.TemplateVdiskID = w.templateVdiskID
	}

	select {
	case w.outputChan <- cfg:
		return nil // ok
	case <-w.ctx.Done():
		return ErrContextDone
	}
}

func (w *nbdStorageConfigWatcher) applyClusterInfo(info VdiskNBDConfig) (bool, error) {
	primaryChanged, err := w.loadPrimaryChan(info.StorageClusterID)
	if err != nil {
		return false, err
	}

	templateClusterChanged, err := w.loadTemplateChan(info.TemplateStorageClusterID)
	if err != nil {
		log.Errorf("error occured in nbdStorageConfigWatcher, slave is invalid: %v", err)
		return primaryChanged, nil
	}

	var templateVdiskIDChanged bool
	if w.templateVdiskID != info.TemplateVdiskID {
		templateVdiskIDChanged = true
		w.templateVdiskID = info.TemplateVdiskID
	}

	changed := primaryChanged || templateClusterChanged || templateVdiskIDChanged
	return changed, nil
}

func (w *nbdStorageConfigWatcher) loadPrimaryChan(clusterID string) (bool, error) {
	if w.primaryClusterID == clusterID {
		return false, nil // nothing to do
	}
	if clusterID == "" {
		return false, ErrNilID
	}

	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		return false, fmt.Errorf(
			"nbdStorageConfigWatcher failed, invalid PrimaryStorageClusterConfig: %v", err)
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-w.primaryChan:
	}

	// primary cluster has been switched successfully
	w.primaryChan = ch
	w.primaryCancel = cancel
	w.primaryStorage = &config
	w.primaryClusterID = clusterID
	return true, nil
}

func (w *nbdStorageConfigWatcher) loadTemplateChan(clusterID string) (bool, error) {
	if w.templateClusterID == clusterID {
		return false, nil // nothing to do
	}

	if clusterID == "" {
		log.Infof(
			"deleting previously used template cluster %s for vdisk %s",
			clusterID, w.vdiskID)
		// meaning we want to delete template cluster
		w.templateClusterID = ""
		w.templateCancel()
		w.templateCancel = nil
		w.templateStorage = nil // make sure storage is set to nil
		return true, nil
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	ch, err := WatchStorageClusterConfig(ctx, w.source, clusterID)
	if err != nil {
		cancel()
		return false, fmt.Errorf(
			"nbdStorageConfigWatcher failed, invalid TemplateStorageClusterConfig: %v", err)
	}

	// read initial value
	var config StorageClusterConfig
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case config = <-w.templateChan:
	}

	// template cluster has been switched successfully
	w.templateChan = ch
	w.templateCancel = cancel
	w.templateStorage = &config
	w.templateClusterID = clusterID
	return true, nil
}
