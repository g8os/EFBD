package ardb

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"golang.org/x/net/context"

	"github.com/garyburd/redigo/redis"

	"github.com/g8os/blockstor/nbdserver/clients/gridapi"
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
	BackendPool    *RedisPool
	GridAPIAddress string
	LBACacheLimit  int64
}

//NewBackend generates a new ardb backend
func (f *BackendFactory) NewBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	volumeID := ec.Name

	//Get information about the volume
	g8osClient := gridapi.NewG8OSStatelessGRID()
	g8osClient.BaseURI = f.GridAPIAddress
	log.Println("[INFO] Starting volume", volumeID)
	volumeInfo, _, err := g8osClient.Volumes.GetVolumeInfo(volumeID, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}

	//Get information about the backend storage nodes
	// TODO: need a way to update while staying alive
	storageClusterInfo, _, err := g8osClient.Storageclusters.GetClusterInfo(volumeInfo.Storagecluster, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}

	redisProvider, err := newRedisProvider(f.BackendPool, storageClusterInfo.DataStorage)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}

	var storage storage
	blockSize := int64(volumeInfo.Blocksize)

	switch volumeInfo.Volumetype {
	case gridapi.EnumVolumeVolumetypedb, gridapi.EnumVolumeVolumetypecache:
		storage = newNonDedupedStorage(volumeID, blockSize, redisProvider)
	case gridapi.EnumVolumeVolumetypeboot:
		cacheLimit := f.LBACacheLimit
		if cacheLimit < lba.BytesPerShard {
			cacheLimit = DefaultLBACacheLimit
		}

		volumeSize := int64(volumeInfo.Size)
		blockCount := volumeSize / blockSize
		if volumeSize%blockSize > 0 {
			blockCount++
		}
		if len(storageClusterInfo.MetadataStorage) < 1 {
			err = fmt.Errorf("No metadata servers available in storagecluster %s", volumeInfo.Storagecluster)
			return
		}
		lbaRedisPool := f.BackendPool.GetConnectionSpecificPool(connectionStringFromHAStorageServer(storageClusterInfo.MetadataStorage[0]))

		var vlba *lba.LBA
		vlba, err = lba.NewLBA(
			volumeID,
			blockCount,
			cacheLimit,
			lbaRedisPool,
		)
		if err != nil {
			log.Println("[ERROR]", err)
			return
		}

		storage = newDedupedStorage(volumeID, blockSize, redisProvider, vlba)
	default:
		err = fmt.Errorf("Unsupported volume type: %s", volumeInfo.Volumetype)
	}

	backend = &Backend{
		blockSize: blockSize,
		size:      uint64(volumeInfo.Size),
		storage:   storage,
	}
	return
}

// newRedisProvider creates a new redis provider
func newRedisProvider(pool *RedisPool, servers []gridapi.HAStorageServer) (*redisProvider, error) {
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
		redisPool:       pool,
		servers:         servers,
		numberOfServers: number,
	}, nil
}

// redisProvider allows you to get a redis connection from a pool
// using a modulo index
type redisProvider struct {
	redisPool       *RedisPool
	servers         []gridapi.HAStorageServer
	numberOfServers int //Keep it as a seperate variable since this is constantly needed
}

// GetRedisConnection from the underlying pool,
// using a modulo index
func (rp *redisProvider) GetRedisConnection(index int) (conn redis.Conn) {
	bcIndex := index % rp.numberOfServers
	conn = rp.redisPool.Get(connectionStringFromHAStorageServer(rp.servers[bcIndex]))
	return
}

func connectionStringFromHAStorageServer(server gridapi.HAStorageServer) string {
	return server.Master.Ip + ":" + strconv.Itoa(server.Master.Port)
}
