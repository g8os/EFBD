package ardb

import (
	"context"
	"errors"
	"fmt"

	log "github.com/glendc/go-mini-log"

	gridapi "github.com/g8os/blockstor/gridapi/gridapiclient"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/blockstor/tlog/tlogclient"
	"github.com/g8os/gonbdserver/nbd"
	"github.com/garyburd/redigo/redis"
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
	SCClientFactory          *storagecluster.ClusterClientFactory
	GridAPIAddress           string
	TLogRPCAddress           string
	RootARDBConnectionString string
	LBACacheLimit            int64 // min-capped to LBA.BytesPerShard
}

// Validate all the parameters of this BackendFactoryConfig
func (cfg *BackendFactoryConfig) Validate() error {
	if cfg.Pool == nil {
		return errors.New("BackendFactory requires a non-nil RedisPool")
	}
	if cfg.SCClientFactory == nil {
		return errors.New("BackendFactory requires a non-nil storage.ClusterConfigFactory")
	}
	if cfg.GridAPIAddress == "" {
		return errors.New("BackendFactory requires a non-empty GridAPIAddress")
	}
	if cfg.TLogRPCAddress == "" {
		return errors.New("BackendFactory requires a non-empty TLogRPCAddress")
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
		backendPool:              cfg.Pool,
		scClientFactory:          cfg.SCClientFactory,
		gridAPIAddress:           cfg.GridAPIAddress,
		tlogRPCAddress:           cfg.TLogRPCAddress,
		rootArdbConnectionString: cfg.RootARDBConnectionString,
		lbaCacheLimit:            cfg.LBACacheLimit,
	}, nil
}

//BackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardbconnections
// I hate the factory pattern but I hate global variables even more
type BackendFactory struct {
	backendPool              *RedisPool
	scClientFactory          *storagecluster.ClusterClientFactory
	gridAPIAddress           string
	tlogRPCAddress           string
	rootArdbConnectionString string
	lbaCacheLimit            int64
}

//NewBackend generates a new ardb backend
func (f *BackendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	// create storage cluster config,
	// which is used to dynamically reload the configuration
	storageClusterClient, err := f.scClientFactory.NewClient(vdiskID)
	if err != nil {
		log.Infof("couldn't get storage cluster client: %s", err.Error())
		return
	}

	redisProvider, err := newRedisProvider(
		f.backendPool, storageClusterClient, f.rootArdbConnectionString)

	//Get information about the vdisk
	g8osClient := gridapi.NewG8OSStatelessGRID()
	g8osClient.BaseURI = f.gridAPIAddress
	log.Info("starting vdisk", vdiskID)
	vdiskInfo, _, err := g8osClient.Vdisks.GetVdiskInfo(vdiskID, nil, nil)
	if err != nil {
		log.Infof("couldn't get vdisk info: %s", err.Error())
		return
	}

	var storage backendStorage
	blockSize := int64(vdiskInfo.Blocksize)

	// GridAPI returns the vdisk size in GiB
	vdiskSize := int64(vdiskInfo.Size) * gibibyteAsBytes

	switch vdiskInfo.Type {
	case gridapi.EnumVdiskTypedb, gridapi.EnumVdiskTypecache:
		storage = newNonDedupedStorage(vdiskID, blockSize, redisProvider)
	case gridapi.EnumVdiskTypeboot:
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
			log.Infof("couldn't create LBA: %s", err.Error())
			return
		}

		storage = newDedupedStorage(vdiskID, blockSize, redisProvider, vlba)
	default:
		err = fmt.Errorf("Unsupported vdisk type: %s", vdiskInfo.Type)
		return
	}

	var tlogClient *tlogclient.Client

	switch vdiskInfo.Type {
	case gridapi.EnumVdiskTypedb, gridapi.EnumVdiskTypeboot:
		log.Debugf("creating tlogclient for backend %v (%v)",
			vdiskID, vdiskInfo.Type)
		tlogClient, err = tlogclient.New(f.tlogRPCAddress)
		if err != nil {
			log.Infof("couldn't create tlogclient: %s", err.Error())
			return
		}

	default:
		log.Debugf("not creating tlogclient for backend %v (%v)",
			vdiskID, vdiskInfo.Type)
	}

	backend = newBackend(
		vdiskID,
		blockSize, uint64(vdiskSize),
		storage,
		storageClusterClient,
		tlogClient,
	)

	return
}

// newRedisProvider creates a new redis provider
func newRedisProvider(pool *RedisPool, storageClusterClient *storagecluster.ClusterClient, rootArdbConnectionString string) (*redisProvider, error) {
	if pool == nil {
		return nil, errors.New(
			"no redis pool is given, while one is required")
	}
	if storageClusterClient == nil {
		return nil, errors.New(
			"no storage cluster client is given, while one is required")
	}

	return &redisProvider{
		redisPool:                pool,
		storageClusterClient:     storageClusterClient,
		rootArdbConnectionString: rootArdbConnectionString,
	}, nil
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
	redisPool                *RedisPool
	storageClusterClient     *storagecluster.ClusterClient
	rootArdbConnectionString string
}

// RedisConnection gets a redis connection from the underlying pool,
// using a modulo index
func (rp *redisProvider) RedisConnection(index int64) (conn redis.Conn, err error) {
	connString, err := rp.storageClusterClient.ConnectionString(index)
	if err != nil {
		return
	}

	conn = rp.redisPool.Get(connString)
	return
}

// FallbackRedisConnection gets a redis connection from the underlying fallback pool,
// using a modulo index
func (rp *redisProvider) FallbackRedisConnection(index int64) (conn redis.Conn, err error) {
	if rp.rootArdbConnectionString == "" {
		err = errNoRootAvailable
		return
	}

	conn = rp.redisPool.Get(rp.rootArdbConnectionString)
	return
}

// MetaRedisConnection implements lba.MetaRedisProvider.MetaRedisConnection
func (rp *redisProvider) MetaRedisConnection() (conn redis.Conn, err error) {
	connString, err := rp.storageClusterClient.MetaConnectionString()
	if err != nil {
		return
	}

	conn = rp.redisPool.Get(connString)
	return
}

var (
	errNoRootAvailable = errors.New("no root ardb connection available")
)
