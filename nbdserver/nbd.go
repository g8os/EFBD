package main

import (
	"log"

	"github.com/g8os/blockstor/nbdserver/clients/storagebackendcontroller"
	"github.com/g8os/blockstor/nbdserver/clients/volumecontroller"
	"github.com/g8os/blockstor/nbdserver/lba"

	"golang.org/x/net/context"

	"github.com/g8os/gonbdserver/nbd"
	"github.com/garyburd/redigo/redis"
)

const (
	//BlockSize is the fixed blocksize for the ardbackend
	BlockSize = 4 * 1024 // 4 kB
	// DefaultLBACacheLimit defines the default cache limit
	DefaultLBACacheLimit = 20 * 1024 * 1024 // 20 mB
)

//ArdbBackend is a nbd.Backend implementation on top of ARDB
type ArdbBackend struct {
	VolumeID string

	BlockSize int64
	Size      uint64
	Deduped   bool
	LBA       *lba.LBA

	RedisConnectionPool *RedisPool

	backendConnectionStrings []storagebackendcontroller.Server
	numberOfStorageServers   int //Keep it as a seperate variable since this is constantly needed

	VolumeControllerClient *volumecontroller.VolumeController
}

//WriteAt implements nbd.Backend.WriteAt
func (ab *ArdbBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (bytesWritten int64, err error) {
	blockIndex := offset / ab.BlockSize
	offsetInsideBlock := offset % ab.BlockSize

	length := int64(len(b))
	if offsetInsideBlock == 0 && length == ab.BlockSize {
		// Option 1.
		// Which is hopefully the most common option
		// in this option we write without an offset,
		// and write a full-sized block, thus no merging required
		err = ab.setContent(ctx, blockIndex, b, fua)

	} else {
		// Option 2.
		// We need to merge both contents.
		// the length can't be bigger, as that is guaranteed by gonbdserver
		err = ab.combineContent(ctx, blockIndex, offsetInsideBlock, b, fua)
	}

	if err != nil {
		return
	}

	bytesWritten = int64(len(b))

	return
}

//ReadAt implements nbd.Backend.ReadAt
func (ab *ArdbBackend) ReadAt(ctx context.Context, b []byte, offset int64) (bytesRead int64, err error) {
	blockIndex := offset / ab.BlockSize
	offsetInsideBlock := offset % ab.BlockSize
	contentLength := int64(len(b))

	contentHash, err := ab.LBA.Get(blockIndex)
	if err != nil {
		return
	}
	if contentHash == nil {
		bytesRead = contentLength
		return
	}

	var block []byte
	if block, err = ab.getContent(contentHash); err != nil {
		if err == redis.ErrNil {
			bytesRead = contentLength
			err = nil
		}

		return
	}

	copy(b, block[offsetInsideBlock:])
	bytesRead = contentLength
	return
}

//TrimAt implements nbd.Backend.TrimAt
func (ab *ArdbBackend) TrimAt(ctx context.Context, offset, length int64) (int64, error) {
	return 0, nil
}

//Flush implements nbd.Backend.Flush
func (ab *ArdbBackend) Flush(ctx context.Context) (err error) {
	err = ab.LBA.Flush()
	return
}

//Close implements nbd.Backend.Close
func (ab *ArdbBackend) Close(ctx context.Context) (err error) {
	return
}

//Geometry implements nbd.Backend.Geometry
func (ab *ArdbBackend) Geometry(ctx context.Context) (nbd.Geometry, error) {
	return nbd.Geometry{
		Size:               ab.Size,
		MinimumBlockSize:   1,
		PreferredBlockSize: uint64(ab.BlockSize),
		MaximumBlockSize:   32 * 1024 * 1024,
	}, nil
}

//HasFua implements nbd.Backend.HasFua
// Yes, we support fua
func (ab *ArdbBackend) HasFua(ctx context.Context) bool {
	return true
}

//HasFlush implements nbd.Backend.HasFlush
// Yes, we support flush
func (ab *ArdbBackend) HasFlush(ctx context.Context) bool {
	return true
}

// get a redis connection based on a hash
func (ab *ArdbBackend) getRedisConnection(hash Hash) (conn redis.Conn) {
	bcIndex := int(hash[0]) % ab.numberOfStorageServers
	conn = ab.RedisConnectionPool.Get(
		ab.backendConnectionStrings[bcIndex].ConnectionString)
	return
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ab *ArdbBackend) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}

// set content as an LBA structure in case the content is not purely 0s, and doesn't exist yet,
// in case it purely filled with 0s, any existing LBA structure will be deleted instead.
func (ab *ArdbBackend) setContent(ctx context.Context, blockIndex int64, content []byte, fua bool) (err error) {
	if ab.isZeroContent(content) {
		err = ab.LBA.Delete(blockIndex)
		if err == nil {
			if fua {
				err = ab.Flush(ctx)
			}
		}

		return
	}

	//Make sure to have a []byte type (so redis does not fall back on slow fmt.Print)
	var hash []byte = HashBytes(content)
	err = func() (err error) {
		conn := ab.getRedisConnection(hash)
		defer conn.Close()

		var exists bool
		exists, err = redis.Bool(conn.Do("EXISTS", hash))
		if err != nil {
			return
		}

		// write content to redis in case it doesn't exist yet
		if !exists {
			_, err = conn.Do("SET", hash, content)
			if err != nil {
				return
			}
		}
		return
	}()
	if err != nil {
		return
	}

	// Write Hash to LBA

	err = ab.LBA.Set(blockIndex, hash)
	if err == nil {
		if fua {
			err = ab.Flush(ctx)
		}
	}

	return
}

// combine the content of an existing block (if it really exists),
// and the newly given content, effectively merging the 2 together,
// and storing them under a new hash.
// NOTE: original content (which is now merged into the new content,
//		 is /NOT/ deleted from Redis. It can't be deleted as conent
//		 could be referenced in multiple shard indices.
func (ab *ArdbBackend) combineContent(ctx context.Context, blockIndex, offset int64, b []byte, fua bool) (err error) {
	hash, _ := ab.LBA.Get(blockIndex)
	content := make([]byte, ab.BlockSize)

	// actual content length,
	// used to cap the content to be written
	var length int64

	if hash != nil {
		// copy original content
		origContent, _ := ab.getContent(hash)
		length = int64(len(origContent))
		copy(content, origContent)
	}

	if l := offset + int64(len(b)); l > length {
		length = l
		if length > ab.BlockSize {
			length = ab.BlockSize
		}
	}

	// copy in new content
	copy(content[offset:], b)

	// store new content
	return ab.setContent(ctx, blockIndex, content[:length], fua)
}

// gets content based on a given hash, if possible
//Make sure to have a []byte type for the hash (so redis does not fall back on slow fmt.Print)
func (ab *ArdbBackend) getContent(hash []byte) (content []byte, err error) {
	conn := ab.getRedisConnection(hash)
	defer conn.Close()

	content, err = redis.Bytes(conn.Do("GET", hash))
	return
}

//ArdbBackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardbconnections
// I hate the factory pattern but I hate global variables even more
type ArdbBackendFactory struct {
	BackendPool              *RedisPool
	volumecontrolleraddress  string
	backendcontrolleraddress string
	lbacachelimit            int64
}

//NewArdbBackend generates a new ardb backend
func (f *ArdbBackendFactory) NewArdbBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	volumeID := ec.Name
	ab := &ArdbBackend{RedisConnectionPool: f.BackendPool}

	//Get information about the volume
	volumeControllerClient := volumecontroller.NewVolumeController()
	volumeControllerClient.BaseURI = f.volumecontrolleraddress
	log.Println("[INFO] Starting volume", volumeID)
	volumeInfo, _, err := volumeControllerClient.Volumes.GetVolumeInfo(volumeID, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}
	ab.Deduped = volumeInfo.Deduped
	ab.BlockSize = int64(volumeInfo.Blocksize)
	ab.Size = uint64(volumeInfo.Size)
	numberOfBlocks := ab.Size / uint64(ab.BlockSize)
	if (ab.Size / uint64(ab.BlockSize)) != 0 {
		numberOfBlocks++
	}

	//Get information about the backend storage nodes
	// TODO: need a way to update while staying alive
	storageBackendClient := storagebackendcontroller.NewStorageBackend()
	storageBackendClient.BaseURI = f.backendcontrolleraddress
	storageClusterInfo, _, err := storageBackendClient.Storagecluster.GetStorageClusterInfo(volumeInfo.Storagecluster, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}
	ab.backendConnectionStrings = storageClusterInfo.Storageservers
	ab.numberOfStorageServers = len(ab.backendConnectionStrings)

	cacheLimit := f.lbacachelimit
	if cacheLimit < lba.BytesPerShard {
		cacheLimit = DefaultLBACacheLimit
	}

	pool := f.BackendPool.GetConnectionSpecificPool(storageClusterInfo.Metadataserver.ConnectionString)
	ab.LBA, err = lba.NewLBA(
		volumeID,
		cacheLimit,
		pool,
	)
	if err != nil {
		return
	}

	backend = ab
	return
}
