package config

import (
	"context"
	"errors"
	"fmt"

	"github.com/zero-os/0-Disk/log"
)

var (
	// ErrWatcherIsBusy is returned from any compose config's watcher
	// where the watcher is already active, when watch is called.
	ErrWatcherIsBusy = errors.New("watcher is already triggered")
)

// WatchTlogStorageConfig watches a given source for TlogStorageConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchTlogStorageConfig(ctx context.Context, source Source, vdiskID string) (<-chan TlogStorageConfigResult, error) {
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
	outputChan chan TlogStorageConfigResult

	// (tlog storage cluster
	tlogChan    <-chan StorageClusterConfigResult
	tlogCancel  func()
	tlogStorage *StorageClusterConfig

	// slave storage cluster
	slaveChan    <-chan StorageClusterConfigResult
	slaveCancel  func()
	slaveStorage *StorageClusterConfig
}

func (w *tlogStorageConfigWatcher) Watch(ctx context.Context) (<-chan TlogStorageConfigResult, error) {
	if w.outputChan != nil {
		return nil, ErrWatcherIsBusy
	}

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

	case clusterInfoResult := <-clusterChan:
		err = clusterInfoResult.Error
		if err == nil {
			_, err = w.applyClusterInfo(*clusterInfoResult.Value)
		}
		if err != nil {
			cancelWatch()
			return nil, fmt.Errorf(
				"TlogStorageConfigWatcher failed, err with initial config: %v", err)
		}
	}

	// create output channel
	w.outputChan = make(chan TlogStorageConfigResult, 1)
	// send initial config
	err = w.sendValue()
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
			case clusterResult := <-w.tlogChan:
				if clusterResult.Error != nil {
					if err := w.sendResult(TlogStorageConfigResult{Error: clusterResult.Error}); err != nil {
						log.Errorf(
							"TlogStorageConfigWatcher failed to send error: %v", err)
						continue
					}
				}
				w.tlogStorage = clusterResult.Value

			// update slave cluster
			case clusterResult := <-w.slaveChan:
				if clusterResult.Error != nil {
					if err := w.sendResult(TlogStorageConfigResult{Error: clusterResult.Error}); err != nil {
						log.Errorf(
							"TlogStorageConfigWatcher failed to send error: %v", err)
						continue
					}
				}
				w.slaveStorage = clusterResult.Value

			// update cluster config
			case clusterInfoResult := <-clusterChan:
				var changed bool
				err := clusterInfoResult.Error
				if err == nil {
					changed, err = w.applyClusterInfo(*clusterInfoResult.Value)
				}
				if err != nil {
					if err = w.sendResult(TlogStorageConfigResult{Error: err}); err != nil {
						log.Errorf(
							"TlogStorageConfigWatcher failed to send error: %v", err)
						continue
					}
				}
				if !changed {
					continue
				}
			}

			// send new output, as a cluster has been updated
			if err := w.sendValue(); err != nil {
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

func (w *tlogStorageConfigWatcher) sendValue() error {
	if w.tlogStorage == nil {
		return errors.New("tlog storage is nil, while it is required")
	}

	cfg := TlogStorageConfig{StorageCluster: w.tlogStorage.Clone()}
	if w.slaveStorage != nil {
		slaveCluster := w.slaveStorage.Clone()
		cfg.SlaveStorageCluster = &slaveCluster
	}

	return w.sendResult(TlogStorageConfigResult{Value: &cfg})
}

func (w *tlogStorageConfigWatcher) sendResult(result TlogStorageConfigResult) error {
	select {
	case w.outputChan <- result:
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
		return tlogChanged, nil // slave can change without it being critical
	}

	changed := tlogChanged || slaveChanged
	return changed, nil
}

func (w *tlogStorageConfigWatcher) loadTlogChan(clusterID string) (changed bool, err error) {
	if w.tlogClusterID == clusterID {
		return // nothing to do
	}
	if w.tlogCancel != nil {
		w.tlogCancel()
		w.tlogCancel = nil
		w.tlogStorage = nil // make sure storage is set to nil
	}

	w.tlogClusterID = clusterID
	if w.tlogClusterID == "" {
		return // nothing to do anymore
	}

	ctx, cancel := context.WithCancel(w.ctx)
	w.tlogChan, err = WatchStorageClusterConfig(ctx, w.source, w.tlogClusterID)
	if err != nil {
		cancel()
		w.tlogClusterID = ""
		err = fmt.Errorf(
			"TlogStorageConfigWatcher failed, invalid TLogStorageClusterConfig: %v", err)
		return
	}

	// read initial value
	var result StorageClusterConfigResult
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case result = <-w.tlogChan:
	}

	changed = true
	w.tlogCancel = cancel

	if result.Error != nil {
		w.tlogStorage = nil
		return changed, result.Error
	}

	w.tlogStorage = result.Value
	return changed, nil
}

func (w *tlogStorageConfigWatcher) loadSlaveChan(clusterID string) (changed bool, err error) {
	if w.slaveClusterID == clusterID {
		return // nothing to do
	}
	if w.slaveCancel != nil {
		w.slaveCancel()
		w.slaveCancel = nil
		w.slaveStorage = nil // make sure storage is set to nil
	}

	// check if the new ID is empty, if so, we're done
	w.slaveClusterID = clusterID
	if w.slaveClusterID == "" {
		return // nothing to do
	}

	// create watch
	ctx, cancel := context.WithCancel(w.ctx)
	w.slaveChan, err = WatchStorageClusterConfig(ctx, w.source, w.slaveClusterID)
	if err != nil {
		cancel()
		w.slaveClusterID = ""
		err = fmt.Errorf(
			"TlogStorageConfigWatcher failed, invalid TLogStorageClusterConfig: %v", err)
		return
	}

	// read initial value
	var result StorageClusterConfigResult
	select {
	case <-ctx.Done():
		cancel()
		return false, ErrContextDone
	case result = <-w.slaveChan:
	}

	changed = true
	w.slaveCancel = cancel

	if result.Error != nil {
		w.slaveStorage = nil
		return changed, result.Error
	}

	w.slaveStorage = result.Value
	return changed, nil
}
