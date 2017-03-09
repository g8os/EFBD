package main

import (
	"fmt"
	"log"

	"github.com/g8os/blockstor/nbdserver/clients/storagebackendcontroller"
	"github.com/g8os/blockstor/nbdserver/clients/volumecontroller"

	"golang.org/x/net/context"

	"github.com/abligh/gonbdserver/nbd"
	"github.com/garyburd/redigo/redis"
)

//BlockSize is the fixed blocksize for the ardbackend
const BlockSize = 4 * 1024

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
func (ab *ArdbBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (bytesWritten int, err error) {
	if (offset%ab.BlockSize) != 0 || len(b) > int(ab.BlockSize) {
		err = fmt.Errorf("Stupid client does not write on block boundary, offset: %d length: %d", offset, len(b))
		return
	}
	contentHash := HashBytes(b)

	//Save to Ardb if this content is not there yet
	conn := ab.RedisConnectionPool.Get(ab.backendConnectionStrings[int(contentHash[0])%ab.numberOfStorageServers].ConnectionString)
	defer conn.Close()
	keysFound, err := redis.Int(conn.Do("EXISTS", *contentHash))
	if err != nil {
		return
	}
	if keysFound == 0 {
		if err = conn.Send("SET", *contentHash, b); err != nil {
			return
		}
		err = conn.Flush()
	}

	//Save hash in the LBA tables
	ab.LBA.Set(offset/ab.BlockSize, contentHash)
	if err == nil {
		bytesWritten = len(b)
	}
	//Flush the LBA structure on fua
	if fua {
		err = ab.Flush(ctx)
	}
	return
}

//ReadAt implements nbd.Backend.ReadAt
func (ab *ArdbBackend) ReadAt(ctx context.Context, b []byte, offset int64) (bytesRead int, err error) {
	blockIndex := offset / ab.BlockSize
	offsetInsideBlock := offset % ab.BlockSize

	contentHash, err := ab.LBA.Get(blockIndex)
	if err != nil {
		return
	}
	if contentHash == nil {
		bytesRead = len(b)
		return
	}

	conn := ab.RedisConnectionPool.Get(ab.backendConnectionStrings[int(contentHash[0])%ab.numberOfStorageServers].ConnectionString)
	defer conn.Close()
	reply, err := conn.Do("GET", *contentHash)
	if err != nil {
		log.Println(reply, err)

	}
	if reply == nil && err == nil {
		bytesRead = len(b)
		return
	}
	block, err := redis.Bytes(reply, err)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r)
			fmt.Println("Offset", offset, "len(b):", len(b), "Reply:", reply, "len(block):", len(block))
		}
	}()

	copy(b, block[offsetInsideBlock:len(block)])
	bytesRead = len(b)
	return
}

//TrimAt implements nbd.Backend.TrimAt
func (ab *ArdbBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
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
func (ab *ArdbBackend) Geometry(ctx context.Context) (uint64, uint64, uint64, uint64, error) {
	return ab.Size, 1, uint64(ab.BlockSize), 32 * 1024 * 1024, nil
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
