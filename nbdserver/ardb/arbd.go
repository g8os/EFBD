package ardb

import (
	"context"
	"errors"
	"fmt"

	log "github.com/glendc/go-mini-log"

	gridapi "github.com/g8os/blockstor/gridapi/gridapiclient"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/storagecluster"
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

// NewBackendFactory creates a new Backend Factory,
// which is used to create a Backend, without having to work with global variables.
func NewBackendFactory(pool *RedisPool, scConfigFactory *storagecluster.ClusterConfigFactory, gridAPIAddress string, lbaCacheLimit int64) (*BackendFactory, error) {
	if pool == nil {
		return nil, errors.New("NewBackendFactory requires a non-nil RedisPool")
	}
	if scConfigFactory == nil {
		return nil, errors.New("NewBackendFactory requires a non-nil storage.ClusterConfigFactory")
	}
	if gridAPIAddress == "" {
		return nil, errors.New("NewBackendFactory requires a non-empty gridAPIAddress")
	}

	return &BackendFactory{
		backendPool:     pool,
		scConfigFactory: scConfigFactory,
		gridAPIAddress:  gridAPIAddress,
		lbaCacheLimit:   lbaCacheLimit,
	}, nil
}

//BackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardbconnections
// I hate the factory pattern but I hate global variables even more
type BackendFactory struct {
	backendPool     *RedisPool
	scConfigFactory *storagecluster.ClusterConfigFactory
	gridAPIAddress  string
	lbaCacheLimit   int64
}

//NewBackend generates a new ardb backend
func (f *BackendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	vdiskID := ec.Name

	// create storage cluster config,
	// which is used to dynamically reload the configuration
	storageClusterCfg, err := f.scConfigFactory.NewConfig(vdiskID)
	if err != nil {
		log.Infof("couldn't get storage cluster info: %s", err.Error())
		return
	}

	redisProvider, err := newRedisProvider(f.backendPool, storageClusterCfg)

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
	}

	backend = &Backend{
		blockSize:         blockSize,
		size:              uint64(vdiskSize),
		storage:           storage,
		storageClusterCfg: storageClusterCfg,
	}
	return
}

// newRedisProvider creates a new redis provider
func newRedisProvider(pool *RedisPool, storageClusterCfg *storagecluster.ClusterConfig) (*redisProvider, error) {
	if pool == nil {
		return nil, errors.New(
			"no redis pool is given, while one is required")
	}
	if storageClusterCfg == nil {
		return nil, errors.New(
			"no storage cluster config is given, while one is required")
	}

	return &redisProvider{
		redisPool:         pool,
		storageClusterCfg: storageClusterCfg,
	}, nil
}

// redisProvider allows you to get a redis connection from a pool
// using a modulo index
type redisProvider struct {
	redisPool         *RedisPool
	storageClusterCfg *storagecluster.ClusterConfig
}

// GetRedisConnection from the underlying pool, using a modulo index
func (rp *redisProvider) RedisConnection(index int) (conn redis.Conn, err error) {
	connString, err := rp.storageClusterCfg.ConnectionString(index)
	if err != nil {
		return
	}

	conn = rp.redisPool.Get(connString)
	return
}

// MetaRedisConnection implements lba.MetaRedisProvider.MetaRedisConnection
func (rp *redisProvider) MetaRedisConnection() (conn redis.Conn, err error) {
	connString, err := rp.storageClusterCfg.MetaConnectionString()
	if err != nil {
		return
	}

	conn = rp.redisPool.Get(connString)
	return
}
