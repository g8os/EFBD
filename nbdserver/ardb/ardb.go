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
	// to convert the vdisk size returned from the GridAPI,
	// from GiB to Bytes
	gibibyteAsBytes int64 = 1024 * 1024 * 1024
	mebibyteAsBytes int64 = 1024 * 1024
)

// BackendFactoryConfig is used to create a new BackendFactory
type BackendFactoryConfig struct {
	Pool                     *RedisPool
	ConfigHotReloader        config.HotReloader
	TLogRPCAddress           string
	RootARDBConnectionString string
	LBACacheLimit            int64 // min-capped to LBA.BytesPerShard
	ConfigPath               string
}

// Validate all the parameters of this BackendFactoryConfig
func (cfg *BackendFactoryConfig) Validate() error {
	if cfg.Pool == nil {
		return errors.New("BackendFactory requires a non-nil RedisPool")
	}
	if cfg.ConfigHotReloader == nil {
		return errors.New("BackendFactory requires a non-nil config.HotReloader")
	}

	return nil
}

// NewBackendFactory creates a new Backend Factory,
// which is used to create a Backend, without having to work with global variables.
func NewBackendFactory(cfg BackendFactoryConfig) (*BackendFactory, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	return &BackendFactory{
		backendPool:    cfg.Pool,
		cfgHotReloader: cfg.ConfigHotReloader,
		tlogRPCAddress: cfg.TLogRPCAddress,
		lbaCacheLimit:  cfg.LBACacheLimit,
		configPath:     cfg.ConfigPath,
	}, nil
}

//BackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardbconnections
// I hate the factory pattern but I hate global variables even more
type BackendFactory struct {
	backendPool    *RedisPool
	cfgHotReloader config.HotReloader
	tlogRPCAddress string
	lbaCacheLimit  int64
	configPath     string
}

//NewBackend generates a new ardb backend
func (f *BackendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	cfg, err := f.cfgHotReloader.VdiskClusterConfig(vdiskID)
	if err != nil {
		log.Error(err)
		return
	}
	vdisk := &cfg.Vdisk

	redisProvider, err := newRedisProvider(f.backendPool, cfg)
	if err != nil {
		log.Error(err)
		return
	}
	go redisProvider.Listen(ctx, vdiskID, f.cfgHotReloader)

	var storage backendStorage
	blockSize := int64(vdisk.BlockSize)

	// GridAPI returns the vdisk size in GiB
	vdiskSize := int64(vdisk.Size) * gibibyteAsBytes

	templateSupport := vdisk.TemplateSupport()

	switch storageType := vdisk.StorageType(); storageType {
	case config.StorageNondeduped:
		storage = newNonDedupedStorage(
			vdiskID, vdisk.RootVdiskID, blockSize, templateSupport, redisProvider)
	case config.StorageDeduped:
		cacheLimit := f.lbaCacheLimit
		if cacheLimit < lba.BytesPerShard {
			log.Infof(
				"NewBackend: LBACacheLimit (%d) will be defaulted to %d (min-capped)",
				cacheLimit, lba.BytesPerShard)
			cacheLimit = DefaultLBACacheLimit
		}

		blockCount := vdiskSize / blockSize
		if vdiskSize%blockSize > 0 {
			blockCount++
		}

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

		storage = newDedupedStorage(
			vdiskID, blockSize, redisProvider, templateSupport, vlba)
	default:
		err = fmt.Errorf("unsupported vdisk storage type %q", storageType)
	}

	if vdisk.TlogSupport() && f.tlogRPCAddress != "" {
		log.Debugf("creating tlogStorage for backend %v (%v)", vdiskID, vdisk.Type)
		storage, err = newTlogStorage(vdiskID, f.tlogRPCAddress, f.configPath, blockSize, storage)
		if err != nil {
			log.Infof("couldn't create tlog storage: %s", err.Error())
			return
		}
	}

	backend = newBackend(
		vdiskID,
		blockSize, uint64(vdiskSize),
		storage,
		redisProvider,
	)

	return
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

func (rp *redisProvider) Close() {
	close(rp.done)
}

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
	errNoRootAvailable = errors.New("no root ardb connection available")
)
