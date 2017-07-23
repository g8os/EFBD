package main

import (
	"context"
	"errors"

	"github.com/zero-os/0-Disk/config"
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
	PoolFactory       ardb.RedisPoolFactory
	ConfigHotReloader config.HotReloader // NBDServer Config Hotreloader
	TLogRPCAddress    string             // address of tlog server
	LBACacheLimit     int64              // min-capped to LBA.BytesPerSector
	ConfigPath        string             // path to the NBDServer's YAML Config
}

// Validate all the parameters of this BackendFactoryConfig,
// returning an error in case the config is invalid.
func (cfg *backendFactoryConfig) Validate() error {
	if cfg.PoolFactory == nil {
		return errors.New("BackendFactory requires a non-nil RedisPoolFactory")
	}
	if cfg.ConfigHotReloader == nil {
		return errors.New("BackendFactory requires a non-nil config.HotReloader")
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
		poolFactory:       cfg.PoolFactory,
		cfgHotReloader:    cfg.ConfigHotReloader,
		cmdTlogRPCAddress: cfg.TLogRPCAddress,
		lbaCacheLimit:     cfg.LBACacheLimit,
		configPath:        cfg.ConfigPath,
		vdiskComp:         &vdiskCompletion{},
	}, nil
}

// backendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardb connections.
// Its NewBackend method is used as the ardb backend generator.
type backendFactory struct {
	poolFactory       ardb.RedisPoolFactory
	cfgHotReloader    config.HotReloader
	cmdTlogRPCAddress string
	lbaCacheLimit     int64
	configPath        string
	vdiskComp         *vdiskCompletion
}

// NewBackend generates a new ardb backend
func (f *backendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	// get the vdisk- and used storage servers configs,
	// which thanks to the hotreloading feature is always up to date
	cfg, err := f.cfgHotReloader.VdiskClusterConfig(vdiskID)
	if err != nil {
		log.Error(err)
		return
	}
	vdisk := &cfg.Vdisk

	// Create a redis provider (with a unique pool),
	// and the found vdisk config.
	// The redisProvider takes care of closing the created redisPool.
	redisPool := f.poolFactory()
	redisProvider, err := ardb.RedisProvider(cfg, redisPool)
	if err != nil {
		log.Error(err)
		return
	}
	go redisProvider.Listen(ctx, vdiskID, f.cfgHotReloader)

	blockSize := int64(vdisk.BlockSize)

	// The NBDServer config defines the vdisk size in GiB,
	// (go)nbdserver however expects it in bytes, thus we have to convert it.
	vdiskSize := int64(vdisk.Size) * ardb.GibibyteAsBytes

	blockStorage, err := storage.NewBlockStorage(
		vdiskID, storage.BlockStorageConfig{
			Vdisk:         *vdisk,
			LBACacheLimit: f.lbaCacheLimit,
		}, redisProvider)
	if err != nil {
		log.Error(err)
		return
	}

	// If the vdisk has tlog support,
	// the storage is wrapped with a tlog storage,
	// which sends all write transactions to the tlog server via an embbed tlog client.
	// One tlog client can define multiple tlog server connections,
	// but only one will be used at a time, the others merely serve as backup servers.
	if vdisk.TlogSupport() {
		if tlogRPCAddrs := f.tlogRPCAddrs(); tlogRPCAddrs != "" {
			log.Debugf("creating tlogStorage for backend %v (%v)", vdiskID, vdisk.Type)
			blockStorage, err = newTlogStorage(vdiskID, tlogRPCAddrs, f.configPath, blockSize, blockStorage)
			if err != nil {
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

// Get the tlog server(s) address(es),
// if tlog rpc addresses are defined via a CLI flag we use those,
// otherwise we try to get it from the latest config.
func (f backendFactory) tlogRPCAddrs() string {
	// return the addresses defined as a CLI flag
	if f.cmdTlogRPCAddress != "" {
		return f.cmdTlogRPCAddress
	}

	// no addresses defined
	if f.configPath == "" {
		return ""
	}

	// return the addresses defined in the TlogServer config,
	// it is however possible that no addresses are defined at all,
	// in which case the returned string will be empty.
	cfg, err := config.ReadConfig(f.configPath, config.TlogServer)
	if err != nil {
		return ""
	}
	return cfg.TlogRPC
}
