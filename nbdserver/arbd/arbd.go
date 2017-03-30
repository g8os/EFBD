package arbd

import (
	"errors"
	"log"

	"golang.org/x/net/context"

	"github.com/garyburd/redigo/redis"

	"github.com/g8os/blockstor/nbdserver/clients/storagebackendcontroller"
	"github.com/g8os/blockstor/nbdserver/clients/volumecontroller"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/gonbdserver/nbd"
)

// shared constants
const (
	// DefaultLBACacheLimit defines the default cache limit
	DefaultLBACacheLimit = 20 * 1024 * 1024 // 20 mB
)

//BackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardbconnections
// I hate the factory pattern but I hate global variables even more
type BackendFactory struct {
	BackendPool              *RedisPool
	VolumeControllerAddress  string
	BackendControllerAddress string
	LBACacheLimit            int64
}

//NewBackend generates a new ardb backend
func (f *BackendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	volumeID := ec.Name

	//Get information about the volume
	volumeControllerClient := volumecontroller.NewVolumeController()
	volumeControllerClient.BaseURI = f.VolumeControllerAddress
	log.Println("[INFO] Starting volume", volumeID)
	volumeInfo, _, err := volumeControllerClient.Volumes.GetVolumeInfo(volumeID, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}

	//Get information about the backend storage nodes
	// TODO: need a way to update while staying alive
	storageBackendClient := storagebackendcontroller.NewStorageBackend()
	storageBackendClient.BaseURI = f.BackendControllerAddress
	storageClusterInfo, _, err := storageBackendClient.Storagecluster.GetStorageClusterInfo(volumeInfo.Storagecluster, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}

	pool := f.BackendPool.GetConnectionSpecificPool(
		storageClusterInfo.Metadataserver.ConnectionString)

	redisProvider, err := newRedisProvider(f.BackendPool, storageClusterInfo.Storageservers)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}

	var storage storage
	blockSize := int64(volumeInfo.Blocksize)

	if !volumeInfo.Deduped {
		storage = newSimpleStorage(volumeID, blockSize, redisProvider)
	} else {
		cacheLimit := f.LBACacheLimit
		if cacheLimit < lba.BytesPerShard {
			cacheLimit = DefaultLBACacheLimit
		}

		volumeSize := int64(volumeInfo.Size)
		blockCount := volumeSize / blockSize
		if volumeSize%blockSize > 0 {
			blockCount++
		}

		var vlba *lba.LBA
		vlba, err = lba.NewLBA(
			volumeID,
			blockCount,
			cacheLimit,
			pool,
		)
		if err != nil {
			log.Println("[ERROR]", err)
			return
		}

		storage = newDedupedStorage(volumeID, blockSize, redisProvider, vlba)
	}

	backend = &Backend{
		blockSize: blockSize,
		size:      uint64(volumeInfo.Size),
		storage:   storage,
	}
	return
}

// newRedisProvider creates a new redis provider
func newRedisProvider(pool *RedisPool, servers []storagebackendcontroller.Server) (*redisProvider, error) {
	if pool == nil {
		return nil, errors.New(
			"no redis pool is given, while one is required")
	}

	number := len(servers)
	if number < 1 {
		return nil, errors.New(
			"received no storageBackendController, while at least 1 is required")
	}

	return &redisProvider{
		redisPool:         pool,
		connectionStrings: servers,
		numberOfServers:   number,
	}, nil
}

// redisProvider allows you to get a redis connection from a pool
// using a modulo index
type redisProvider struct {
	redisPool         *RedisPool
	connectionStrings []storagebackendcontroller.Server
	numberOfServers   int //Keep it as a seperate variable since this is constantly needed
}

// GetRedisConnection from the underlying pool,
// using a modulo index
func (rp *redisProvider) GetRedisConnection(index int) (conn redis.Conn) {
	bcIndex := index % rp.numberOfServers
	conn = rp.redisPool.Get(rp.connectionStrings[bcIndex].ConnectionString)
	return
}
