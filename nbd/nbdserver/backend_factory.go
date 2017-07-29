package main

import (
	"context"
	"errors"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
)

// backendFactoryConfig is used to create a new BackendFactory
type backendFactoryConfig struct {
	// Redis pool factory used to create the redis (= storage servers) pool
	// a factory is used, rather than a shared redis pool,
	// such that each vdisk (session) will get its own redis pool.
	PoolFactory   ardb.RedisPoolFactory
	LBACacheLimit int64               // min-capped to LBA.BytesPerSector
	ConfigInfo    zerodisk.ConfigInfo // config source info
}

// Validate all the parameters of this BackendFactoryConfig,
// returning an error in case the config is invalid.
func (cfg *backendFactoryConfig) Validate() error {
	if cfg.PoolFactory == nil {
		return errors.New("BackendFactory requires a non-nil RedisPoolFactory")
	}
	if err := cfg.ConfigInfo.Validate(); err != nil {
		return errors.New(
			"BackendFactory requires valid config info: " + err.Error())
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
		poolFactory:   cfg.PoolFactory,
		lbaCacheLimit: cfg.LBACacheLimit,
		configInfo:    cfg.ConfigInfo,
		vdiskComp:     &vdiskCompletion{},
	}, nil
}

// backendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardb connections.
// Its NewBackend method is used as the ardb backend generator.
type backendFactory struct {
	poolFactory   ardb.RedisPoolFactory
	lbaCacheLimit int64
	configInfo    zerodisk.ConfigInfo
	vdiskComp     *vdiskCompletion
}

// NewBackend generates a new ardb backend
func (f *backendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	// fetch base config
	baseCfg, nbdCfg, err := zerodisk.ReadNBDConfig(vdiskID, f.configInfo)
	if err != nil {
		log.Error(err)
		return
	}

	// Create a redis provider (with a unique pool),
	// and the found vdisk config.
	// The redisProvider takes care of closing the created redisPool.
	// The redisProvider created here also supports hot reloading.
	redisPool := f.poolFactory()
	redisProvider, err := ardb.DynamicProvider(ctx, vdiskID, f.configInfo, redisPool)
	if err != nil {
		log.Error(err)
		return
	}

	blockSize := int64(baseCfg.BlockSize)

	// The NBDServer config defines the vdisk size in GiB,
	// (go)nbdserver however expects it in bytes, thus we have to convert it.
	vdiskSize := int64(baseCfg.Size) * ardb.GibibyteAsBytes

	blockStorage, err := storage.NewBlockStorage(
		storage.BlockStorageConfig{
			VdiskID:         vdiskID,
			TemplateVdiskID: nbdCfg.TemplateVdiskID,
			VdiskType:       baseCfg.Type,
			VdiskSize:       vdiskSize,
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
	if baseCfg.Type.TlogSupport() {
		if nbdCfg.TlogServerAddresses != nil {
			log.Debugf("creating tlogStorage for backend %v (%v)", vdiskID, baseCfg.Type)
			blockStorage, err = newTlogStorage(
				vdiskID, nbdCfg.TlogServerAddresses, &f.configInfo, blockSize, blockStorage)
			if err != nil {
				blockStorage.Close()
				redisProvider.Close()
				log.Infof("couldn't create tlog storage: %s", err.Error())
				return
			}
		}
	}

	// Create the actual ARDB backend
	backend = newBackend(
		vdiskID,
		uint64(vdiskSize), blockSize,
		blockStorage,
		f.vdiskComp,
		redisProvider,
	)

	return
}

// Wait waits for vdisks completion.
// It only wait for vdisk which has vdiskCompletion
// attached.
// It returns errors from vdisk that exited
// because of context cancellation.
func (f backendFactory) Wait() []error {
	return f.vdiskComp.Wait()
}
