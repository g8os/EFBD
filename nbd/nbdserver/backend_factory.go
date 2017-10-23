package main

import (
	"context"
	"errors"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/nbd/nbdserver/statistics"
	"github.com/zero-os/0-Disk/nbd/nbdserver/tlog"
)

// backendFactoryConfig is used to create a new BackendFactory
type backendFactoryConfig struct {
	LBACacheLimit int64         // min-capped to LBA.BytesPerSector
	ConfigSource  config.Source // config source
}

// Validate all the parameters of this BackendFactoryConfig,
// returning an error in case the config is invalid.
func (cfg *backendFactoryConfig) Validate() error {
	if cfg.ConfigSource == nil {
		return errors.New("BackendFactory requires a non-nil config source")
	}

	return nil
}

// newBackendFactory creates a new Backend Factory,
// which is used to create a Backend, without having to work with global variables.
// Returns an error in case the given BackendFactoryConfig is invalid.
func newBackendFactory(cfg backendFactoryConfig) (*backendFactory, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	return &backendFactory{
		lbaCacheLimit: cfg.LBACacheLimit,
		configSource:  cfg.ConfigSource,
		vdiskComp:     newVdiskCompletion(),
	}, nil
}

// backendFactory holds some variables
// that can not be passed in the exportconfig like the config source.
// Its NewBackend method is used as the ardb backend generator.
type backendFactory struct {
	lbaCacheLimit int64
	configSource  config.Source
	vdiskComp     *vdiskCompletion
}

type closers []Closer

func (cs closers) Close() error {
	var err error
	for _, c := range cs {
		err = c.Close()
		if err != nil {
			log.Errorf("error while closing closer: %v", err)
		}
	}

	return nil
}

// NewBackend generates a new ardb backend
func (f *backendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	log.Infof("creating new backend for vdisk `%v`", vdiskID)

	// fetch static config
	staticConfig, err := config.ReadVdiskStaticConfig(f.configSource, vdiskID)
	if err != nil {
		log.Error(err)
		return
	}

	blockSize := int64(staticConfig.BlockSize)

	var resourceCloser closers

	// create primary cluster
	primaryCluster, err := storage.NewPrimaryCluster(ctx, vdiskID, f.configSource)
	if err != nil {
		log.Error(err)
		return
	}
	resourceCloser = append(resourceCloser, primaryCluster)

	// create template cluster if supported by vdisk
	// NOTE: internal template cluster may be nil, this is OK
	var templateCluster *storage.TemplateCluster
	if staticConfig.Type.TemplateSupport() {
		templateCluster, err = storage.NewTemplateCluster(ctx, vdiskID, f.configSource)
		if err != nil {
			resourceCloser.Close()
			log.Error(err)
			return
		}
		resourceCloser = append(resourceCloser, templateCluster)
	}

	blockStorage, err := storage.NewBlockStorage(
		storage.BlockStorageConfig{
			VdiskID:         vdiskID,
			TemplateVdiskID: staticConfig.TemplateVdiskID,
			VdiskType:       staticConfig.Type,
			BlockSize:       blockSize,
			LBACacheLimit:   f.lbaCacheLimit,
		}, primaryCluster, templateCluster)
	if err != nil {
		resourceCloser.Close()
		log.Error(err)
		return
	}

	// If the vdisk has tlog support,
	// the storage is wrapped with a tlog storage,
	// which sends all write transactions to the tlog server via an embbed tlog client.
	// One tlog client can define multiple tlog server connections,
	// but only one will be used at a time, the others merely serve as backup servers.
	if staticConfig.Type.TlogSupport() {
		vdiskNBDConfig, err := config.ReadVdiskNBDConfig(f.configSource, vdiskID)
		if err != nil {
			blockStorage.Close()
			resourceCloser.Close()
			log.Infof("couldn't vdisk %s's NBD config: %s", vdiskID, err.Error())
			return nil, err
		}
		if vdiskNBDConfig.TlogServerClusterID != "" {
			log.Infof("creating tlogStorage for backend %v (%v)", vdiskID, staticConfig.Type)
			tlogBlockStorage, err := tlog.Storage(ctx,
				vdiskID,
				f.configSource, blockSize, blockStorage, primaryCluster, nil)
			if err != nil {
				blockStorage.Close()
				resourceCloser.Close()
				log.Infof("couldn't create tlog storage: %s", err.Error())
				return nil, err
			}
			blockStorage = tlogBlockStorage
		}
	}

	// create statistics loggers
	vdiskLogger, err := statistics.NewVdiskLogger(ctx, f.configSource, vdiskID)
	if err != nil {
		blockStorage.Close()
		resourceCloser.Close()
		log.Infof("couldn't create vdisk logger: %s", err.Error())
		return nil, err
	}

	// Create the actual ARDB backend
	backend = newBackend(
		vdiskID,
		staticConfig.Size*uint64(ardb.GibibyteAsBytes),
		blockSize,
		blockStorage,
		f.vdiskComp,
		resourceCloser,
		vdiskLogger,
	)

	return
}

// StopAndWait stops all vdisk and waits for vdisks completion.
// It only stop and wait for vdisk which has vdiskCompletion
// attached.
// It returns errors from vdisk that exited
// because of context cancellation.
func (f backendFactory) StopAndWait() []error {
	f.vdiskComp.StopAll()
	return f.vdiskComp.Wait()
}
