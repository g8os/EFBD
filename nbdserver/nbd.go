package main

import (
	"fmt"
	"log"

	"github.com/g8os/blockstor/nbdserver/clients/storagebackendcontroller"
	"github.com/g8os/blockstor/nbdserver/clients/volumecontroller"

	"golang.org/x/net/context"

	"github.com/g8os/gonbdserver/nbd"
	"github.com/garyburd/redigo/redis"
)

//BlockSize is the fixed blocksize for the ardbackend
const BlockSize = 4 * 1024 // 4kB

//ArdbBackend is a nbd.Backend implementation on top of ARDB
type ArdbBackend struct {
	VolumeID string

	BlockSize int64
	Size      uint64
	Deduped   bool
	LBA       *LBA

	RedisConnectionPool *RedisPool

	backendConnectionStrings []storagebackendcontroller.Server
	numberOfStorageServers   int //Keep it as a seperate variable since this is constantly needed

	VolumeControllerClient *volumecontroller.VolumeController
}

//WriteAt implements nbd.Backend.WriteAt
func (ab *ArdbBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (bytesWritten int64, err error) {
	blockIndex := offset / ab.BlockSize
	offsetInsideBlock := offset % ab.BlockSize

	if offsetInsideBlock == 0 {
		length := int64(len(b))
		if length == ab.BlockSize {
			// Option 1.
			// Which is hopefully the most common option
			// in this option we write without an offset,
			// and write a full-sized block, thus no merging required
			err = ab.setContent(ctx, blockIndex, b, fua)
			if err != nil {
				return
			}

			bytesWritten = int64(len(b))
			return
		}

		// Option 2.
		// We have no offset, but the length is smaller then ab.BlockSize,
		// thus we will merge both contents.
		// the length can't be bigger, as that is guaranteed by gonbdserver
		err = ab.combineContent(ctx, blockIndex, length, b, fua)
		if err != nil {
			return
		}

		bytesWritten = int64(len(b))
		return
	}

	if offsetInsideBlock+int64(len(b)) <= ab.BlockSize {
		// Option 3.
		// We have an offset > 0, and it fits 1 one block
		// requires a bit of copying, though.
		// On top of that it might also require an extra read,
		// in case we have already content written
		err = ab.combineContent(ctx, blockIndex, offsetInsideBlock, b, fua)
		if err != nil {
			return
		}

		bytesWritten = int64(len(b))
		return
	}

	// Option 4.
	// We have an offset > 0, and it requires 2 blocks

	// this one will require 2 WriteAt operations
	length := ab.BlockSize - offsetInsideBlock
	// this first write operation, also stores the hash in LBA,
	// for the first half of the given content
	bytesWritten, err = ab.WriteAt(ctx, b[:length], offset, fua)
	if err != nil {
		bytesWritten = 0
		return
	}
	if bytesWritten != length {
		err = fmt.Errorf("write 1/2 wrote %d bytes, while expected to write %d bytes",
			bytesWritten, length)
		bytesWritten = 0
		return
	}

	// reset output variable, to start clean
	bytesWritten = 0

	// write part 2/2,
	// merging any existing content, which starts at `> length`
	// a process very similar to what happens in option 2 of this method
	content := b[length:]
	offsetInsideBlock = int64(len(content))
	// we need to move up 1 block index,
	// as LBA is set later, using this index
	blockIndex++
	// do the actual combining and writing (if possible)
	err = ab.combineContent(ctx, blockIndex, offsetInsideBlock, content, fua)
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

	// with a local offset neq 0,
	// we could end up in a situation where the wanted content
	// stretches 2 blocks, not 1.
	// This branch handles that case by delegating the first portion,
	// and reading the rest as usual
	if offsetInsideBlock+contentLength > ab.BlockSize {
		length := ab.BlockSize - offset
		bytesRead, err = ab.ReadAt(ctx, b[:length], offset)
		if err != nil {
			bytesRead = 0
			return
		}
		if bytesRead != length {
			err = fmt.Errorf("read 1/2 read %d bytes, while expected to write %d bytes",
				bytesRead, length)
			bytesRead = 0
			return
		}

		// prepare buffer, offset and index for read 2/2
		b = b[length:]
		contentLength = ab.BlockSize - length
		offsetInsideBlock = 0
		bytesRead = 0
		blockIndex++
	}

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
func (ab *ArdbBackend) getRedisConnection(hash *Hash) (conn redis.Conn) {
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

	hash := HashBytes(content)
	conn := ab.getRedisConnection(hash)
	defer conn.Close()

	var exists bool
	exists, err = redis.Bool(conn.Do("EXISTS", *hash))
	if err != nil {
		return
	}

	// write content to redis in case it doesn't exist yet
	if !exists {
		_, err = conn.Do("SET", *hash, content)
		if err != nil {
			return
		}
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
func (ab *ArdbBackend) getContent(hash *Hash) (content []byte, err error) {
	conn := ab.getRedisConnection(hash)
	defer conn.Close()

	content, err = redis.Bytes(conn.Do("GET", *hash))
	return
}

//ArdbBackendFactory holds some variables
// that can not be passed in the exportconfig like the pool of ardbconnections
// I hate the factory pattern but I hate global variables even more
type ArdbBackendFactory struct {
	BackendPool *RedisPool
}

//NewArdbBackend generates a new ardb backend
func (f *ArdbBackendFactory) NewArdbBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	volumeID := ec.Name
	ab := &ArdbBackend{RedisConnectionPool: f.BackendPool}

	//Get information about the volume
	volumeControllerClient := volumecontroller.NewVolumeController()
	volumeControllerClient.BaseURI = ec.DriverParameters["volumecontrolleraddress"]
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
	storageBackendClient.BaseURI = ec.DriverParameters["backendcontrolleraddress"]
	storageClusterInfo, _, err := storageBackendClient.Storagecluster.GetStorageClusterInfo(volumeInfo.Storagecluster, nil, nil)
	if err != nil {
		log.Println("[ERROR]", err)
		return
	}
	ab.backendConnectionStrings = storageClusterInfo.Storageservers
	ab.numberOfStorageServers = len(ab.backendConnectionStrings)

	ab.LBA = NewLBA(volumeID, numberOfBlocks, f.BackendPool.GetConnectionSpecificPool(storageClusterInfo.Metadataserver.ConnectionString))

	backend = ab
	return
}
