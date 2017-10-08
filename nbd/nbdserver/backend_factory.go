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
// that can not be passed in the exportconfig like the pool of ardb connections.
// Its NewBackend method is used as the ardb backend generator.
type backendFactory struct {
	lbaCacheLimit int64
	configSource  config.Source
	vdiskComp     *vdiskCompletion
}

// NewBackend generates a new ardb backend
func (f *backendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	// fetch static config
	staticConfig, err := config.ReadVdiskStaticConfig(f.configSource, vdiskID)
	if err != nil {
		log.Error(err)
		return
	}

	// Create a redis provider (with a unique pool),
	// and the found vdisk config.
	// The redisProvider takes care of closing the created redisPool.
	// The redisProvider created here also supports hot reloading.
	redisPool := ardb.NewRedisPool(nil)
	redisProvider, err := ardb.DynamicProvider(ctx, vdiskID, f.configSource, redisPool)
	if err != nil {
		log.Error(err)
		return
	}

	blockSize := int64(staticConfig.BlockSize)

	// The NBDServer config defines the vdisk size in GiB,
	// (go)nbdserver however expects it in bytes, thus we have to convert it.
	vdiskSize := int64(staticConfig.Size) * ardb.GibibyteAsBytes

	blockStorage, err := storage.NewBlockStorage(
		storage.BlockStorageConfig{
			VdiskID:         vdiskID,
			TemplateVdiskID: staticConfig.TemplateVdiskID,
			VdiskType:       staticConfig.Type,
			BlockSize:       blockSize,
			LBACacheLimit:   f.lbaCacheLimit,
		}, redisProvider)
	if err != nil {
		redisProvider.Close()
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
			redisProvider.Close()
			log.Infof("couldn't vdisk %s's NBD config: %s", vdiskID, err.Error())
			return nil, err
		}
		if vdiskNBDConfig.TlogServerClusterID != "" {
			log.Debugf("creating tlogStorage for backend %v (%v)", vdiskID, staticConfig.Type)
			tlogBlockStorage, err := tlog.Storage(ctx,
				vdiskID, vdiskNBDConfig.TlogServerClusterID,
				f.configSource, blockSize, blockStorage, redisProvider, nil)
			if err != nil {
				blockStorage.Close()
				redisProvider.Close()
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
		redisProvider.Close()
		log.Infof("couldn't create vdisk logger: %s", err.Error())
		return nil, err
	}

	// Create the actual ARDB backend
	backend = newBackend(
		vdiskID,
		uint64(vdiskSize), blockSize,
		blockStorage,
		f.vdiskComp,
		redisProvider,
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
