package ardb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/zero-os/0-Disk/log"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/nbdserver/lba"
)

// shared constants
const (
	// DefaultLBACacheLimit defines the default cache limit
	DefaultLBACacheLimit = 20 * mebibyteAsBytes // 20 MiB
	// constants used to convert between MiB/GiB and bytes
	gibibyteAsBytes int64 = 1024 * 1024 * 1024
	mebibyteAsBytes int64 = 1024 * 1024
)

// BackendFactoryConfig is used to create a new BackendFactory
type BackendFactoryConfig struct {
	// Redis pool factory used to create the redis (= storage servers) pool
	// a factory is used, rather than a shared redis pool,
	// such that each vdisk (session) will get its own redis pool.
	PoolFactory       RedisPoolFactory
	ConfigHotReloader config.HotReloader // NBDServer Config Hotreloader
	TLogRPCAddress    string             // address of tlog server
	LBACacheLimit     int64              // min-capped to LBA.BytesPerShard
	ConfigPath        string             // path to the NBDServer's YAML Config
}

// Validate all the parameters of this BackendFactoryConfig,
// returning an error in case the config is invalid.
func (cfg *BackendFactoryConfig) Validate() error {
	if cfg.PoolFactory == nil {
		return errors.New("BackendFactory requires a non-nil RedisPoolFactory")
	}
	if cfg.ConfigHotReloader == nil {
		return errors.New("BackendFactory requires a non-nil config.HotReloader")
	}

	return nil
}

// NewBackendFactory creates a new Backend Factory,
// which is used to create a Backend, without having to work with global variables.
// Returns an error in case the given BackendFactoryConfig is invalid.
func NewBackendFactory(cfg BackendFactoryConfig) (*BackendFactory, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	return &BackendFactory{
		poolFactory:       cfg.PoolFactory,
		cfgHotReloader:    cfg.ConfigHotReloader,
		cmdTlogRPCAddress: cfg.TLogRPCAddress,
		lbaCacheLimit:     cfg.LBACacheLimit,
		configPath:        cfg.ConfigPath,
	}, nil
}

// BackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardb connections.
// Its NewBackend method is used as the ardb backend generator.
type BackendFactory struct {
	poolFactory       RedisPoolFactory
	cfgHotReloader    config.HotReloader
	cmdTlogRPCAddress string
	lbaCacheLimit     int64
	configPath        string
}

// NewBackend generates a new ardb backend
func (f *BackendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
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
	redisProvider, err := newRedisProvider(redisPool, cfg)
	if err != nil {
		log.Error(err)
		return
	}
	go redisProvider.Listen(ctx, vdiskID, f.cfgHotReloader)

	var storage backendStorage
	blockSize := int64(vdisk.BlockSize)

	// The NBDServer config defines the vdisk size in GiB,
	// (go)nbdserver however expects it in bytes, thus we have to convert it.
	vdiskSize := int64(vdisk.Size) * gibibyteAsBytes

	templateSupport := vdisk.TemplateSupport()

	// create the actual storage backend used for this vdisk,
	// which storage is used, is defined by the vdisk's storage type,
	// which is in function of the vdisk's properties.
	switch storageType := vdisk.StorageType(); storageType {
	// non deduped storage
	case config.StorageNondeduped:
		storage = newNonDedupedStorage(
			vdiskID, vdisk.RootVdiskID, blockSize, templateSupport, redisProvider)

	// deduped storage
	case config.StorageDeduped:
		// define the LBA cache limit
		cacheLimit := f.lbaCacheLimit
		if cacheLimit < lba.BytesPerShard {
			log.Infof(
				"NewBackend: LBACacheLimit (%d) will be defaulted to %d (min-capped)",
				cacheLimit, lba.BytesPerShard)
			cacheLimit = DefaultLBACacheLimit
		}

		// define the block count based on the vdisk size and block size
		blockCount := vdiskSize / blockSize
		if vdiskSize%blockSize > 0 {
			blockCount++
		}

		// create the LBA (used to store deduped metadata)
		var vlba *lba.LBA
		vlba, err = lba.NewLBA(
			vdiskID,
			blockCount,
			cacheLimit,
			redisProvider,
		)
		if err != nil {
			log.Errorf("couldn't create LBA: %s", err.Error())
			return
		}

		// create the actual deduped storage
		storage = newDedupedStorage(
			vdiskID, blockSize, redisProvider, templateSupport, vlba)

	default:
		err = fmt.Errorf("unsupported vdisk storage type %q", storageType)
	}

	// If the vdisk has tlog support,
	// the storage is wrapped with a tlog storage,
	// which sends all write transactions to the tlog server via an embbed tlog client.
	// One tlog client can define multiple tlog server connections,
	// but only one will be used at a time, the others merely serve as backup servers.
	if vdisk.TlogSupport() {
		var tlogRPCAddrs string

		tlogRPCAddrs, err = f.tlogRPCAddrs()
		if err != nil {
			log.Errorf("couldn't create tlog storage. invalid TLogRPCAddress: %s", err.Error())
			return
		}

		if tlogRPCAddrs != "" {
			log.Debugf("creating tlogStorage for backend %v (%v)", vdiskID, vdisk.Type)
			storage, err = newTlogStorage(vdiskID, tlogRPCAddrs, f.configPath, blockSize, storage)
			if err != nil {
				log.Infof("couldn't create tlog storage: %s", err.Error())
				return
			}
		}
	}

	// Create the actual ARDB backend
	backend = newBackend(
		vdiskID,
		blockSize, uint64(vdiskSize),
		storage,
		redisProvider,
	)

	return
}

// Get the tlog server(s) address(es),
// if tlog rpc addresses are defined via a CLI flag we use those,
// otherwise we try to get it from the latest config.
func (f BackendFactory) tlogRPCAddrs() (string, error) {
	// return the addresses defined as a CLI flag
	if f.cmdTlogRPCAddress != "" {
		return f.cmdTlogRPCAddress, nil
	}

	// no addresses defined
	if f.configPath == "" {
		return "", nil
	}

	// return the addresses defined in the TlogServer config,
	// it is however possible that no addresses are defined at all,
	// in which case the returned string will be empty.
	cfg, err := config.ReadConfig(f.configPath, config.TlogServer)
	if err != nil {
		return "", err
	}
	return cfg.TlogRPC, nil
}

// newRedisProvider creates a new redis provider
func newRedisProvider(pool *RedisPool, cfg *config.VdiskClusterConfig) (*redisProvider, error) {
	if pool == nil {
		return nil, errors.New(
			"no redis pool is given, while one is required")
	}

	provider := &redisProvider{
		redisPool: pool,
		done:      make(chan struct{}),
	}

	// load nbdserver config
	err := provider.reloadConfig(cfg)
	if err != nil {
		return nil, err
	}

	return provider, nil
}

// redisConnectionProvider defines the interface to get a redis connection,
// based on a given index, used by the arbd storage backends
type redisConnectionProvider interface {
	RedisConnection(index int64) (conn redis.Conn, err error)
	FallbackRedisConnection(index int64) (conn redis.Conn, err error)
}

// redisProvider allows you to get a redis connection from a pool
// using a modulo index
type redisProvider struct {
	redisPool *RedisPool

	done chan struct{}

	// used to get a redis connection
	dataConnectionConfigs []config.StorageServerConfig
	numberOfServers       int64 //Keep it as a seperate variable since this is constantly needed

	// used as a fallback for getting data
	// from a remote (root/template) server
	rootDataConnectionConfigs []config.StorageServerConfig
	numberOfRootServers       int64 //Keep it as a seperate variable since this is constantly needed

	// used to store meta data
	metaConnectionConfig *config.StorageServerConfig

	// to protect connection configs
	mux sync.RWMutex
}

// RedisConnection gets a redis connection from the underlying pool,
// using a modulo index
func (rp *redisProvider) RedisConnection(index int64) (conn redis.Conn, err error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()

	bcIndex := index % rp.numberOfServers
	connConfig := &rp.dataConnectionConfigs[bcIndex]

	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// FallbackRedisConnection gets a redis connection from the underlying fallback pool,
// using a modulo index
func (rp *redisProvider) FallbackRedisConnection(index int64) (conn redis.Conn, err error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()

	// not all vdisks have a rootStoragecluster defined,
	// it is therefore not a guarantee that at least one server is available,
	// a given we do have in the ConnectionString method
	if rp.numberOfRootServers < 1 {
		err = errNoRootAvailable
		return
	}

	bcIndex := index % rp.numberOfRootServers
	connConfig := &rp.rootDataConnectionConfigs[bcIndex]

	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// MetaRedisConnection implements lba.MetaRedisProvider.MetaRedisConnection
func (rp *redisProvider) MetaRedisConnection() (conn redis.Conn, err error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()

	connConfig := rp.metaConnectionConfig
	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// Listen to the config hot reloader,
// and reload the nbdserver config incase it has been updated.
func (rp *redisProvider) Listen(ctx context.Context, vdiskID string, hr config.HotReloader) {
	log.Debug("redisProvider listening")

	ch := make(chan config.VdiskClusterConfig)
	err := hr.Subscribe(ch, vdiskID)
	if err != nil {
		panic(err)
	}

	defer func() {
		log.Debug("exit redisProvider listener")
		if err := hr.Unsubscribe(ch); err != nil {
			log.Error(err)
		}
		close(ch)
	}()

	for {
		select {
		case cfg := <-ch:
			err := rp.reloadConfig(&cfg)
			if err != nil {
				log.Error(err)
			}

		case <-rp.done:
			return

		case <-ctx.Done():
			return
		}
	}
}

// Close the internal redis pool and stop the listen goroutine.
func (rp *redisProvider) Close() {
	close(rp.done)
	rp.redisPool.Close()
}

// Update all internally stored parameters we care about,
// based on the updated config content.
func (rp *redisProvider) reloadConfig(cfg *config.VdiskClusterConfig) error {
	rp.mux.Lock()
	defer rp.mux.Unlock()

	rp.dataConnectionConfigs = cfg.DataCluster.DataStorage
	rp.numberOfServers = int64(len(rp.dataConnectionConfigs))

	rp.metaConnectionConfig = cfg.DataCluster.MetadataStorage

	if cfg.RootCluster == nil {
		rp.rootDataConnectionConfigs = nil
		rp.numberOfRootServers = 0
	} else {
		rp.rootDataConnectionConfigs = cfg.RootCluster.DataStorage
		rp.numberOfRootServers = int64(len(rp.rootDataConnectionConfigs))
	}

	return nil
}

var (
	// error returned when no root storage cluster has been defined,
	// while a connection to it was requested.
	errNoRootAvailable = errors.New("no root ardb connection available")
)
