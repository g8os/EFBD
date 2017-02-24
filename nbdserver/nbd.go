package main

import (
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"

	"github.com/abligh/gonbdserver/nbd"
	"github.com/garyburd/redigo/redis"
)

//BlockSize is the fixed blocksize for the ardbackend
const BlockSize = 4 * 1024

//ArdbBackend is a nbd.Backend implementation on top of ARDB
type ArdbBackend struct {
	BlockSize int64
	Size      uint64
	LBA       *LBA
	//TODO: should be pool of different ardb's
	Connections *redis.Pool
}

//WriteAt implements nbd.Backend.WriteAt
func (ab *ArdbBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (bytesWritten int, err error) {
	if (offset%ab.BlockSize) != 0 || len(b) > int(ab.BlockSize) {
		err = fmt.Errorf("Stupid client does not write on block boundary, offset: %d length: %d", offset, len(b))
		return
	}
	contentHash := HashBytes(b)

	//Save to Ardb
	conn := ab.Connections.Get()
	defer conn.Close()
	conn.Send("SET", contentHash, b)
	err = conn.Flush()

	//Save hash in the LBA tables
	ab.LBA.Set(offset/ab.BlockSize, contentHash)

	if err == nil {
		bytesWritten = len(b)
	}
	return
}

//ReadAt implements nbd.Backend.ReadAt
func (ab *ArdbBackend) ReadAt(ctx context.Context, b []byte, offset int64) (bytesRead int, err error) {
	blockIndex := offset / ab.BlockSize
	offsetInsideBlock := offset % ab.BlockSize
	log.Println("Reading block at offset", offset, "(in block:", offsetInsideBlock, ") len", len(b))

	contentHash := ab.LBA.Get(blockIndex)
	if contentHash == nil {
		bytesRead = len(b)
		return
	}

	conn := ab.Connections.Get()
	defer conn.Close()
	reply, err := conn.Do("GET", contentHash)
	if err != nil {
		log.Println(reply, err)

	}
	if reply == nil && err == nil {
		bytesRead = len(b)
		return
	}
	block, err := redis.Bytes(reply, err)
	copy(b, block[offsetInsideBlock:int(offsetInsideBlock)+len(b)])
	bytesRead = len(b)
	return
}

//TrimAt implements nbd.Backend.TrimAt
func (ab *ArdbBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
	return 0, nil
}

//Flush implements nbd.Backend.Flush
func (ab *ArdbBackend) Flush(ctx context.Context) (err error) {
	return
}

//Close implements nbd.Backend.Close
func (ab *ArdbBackend) Close(ctx context.Context) (err error) {
	if ab.Connections != nil {
		ab.Connections.Close()
	}
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

//NewArdbBackend generates a new ardb backend
func NewArdbBackend(ctx context.Context, ec *nbd.ExportConfig) (backend nbd.Backend, err error) {
	//TODO: get diskSize from external volume information service
	var diskSize uint64 = 20000000000
	numberOfBlocks := diskSize / BlockSize
	if (diskSize % BlockSize) != 0 {
		numberOfBlocks++
	}
	ab := &ArdbBackend{BlockSize: BlockSize, Size: diskSize, LBA: NewLBA(numberOfBlocks)}
	//TODO: should be pool of different ardb's
	ab.Connections = &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", "localhost:16379") },
	}
	backend = ab
	return
}

// Register our backend
func init() {
	nbd.RegisterBackend("ardb", NewArdbBackend)
}
