package main

import (
	"log"
	"time"

	"golang.org/x/net/context"

	"github.com/abligh/gonbdserver/nbd"
	"github.com/garyburd/redigo/redis"
)

//ArdbBackend is a nbd.Backend implementation on top of ARDB
type ArdbBackend struct {
	BlockSize int

	//TODO: should be pool of different ardb's
	Connections *redis.Pool
}

//WriteAt implements nbd.Backend.WriteAt
func (ab *ArdbBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (bytesWritten int, err error) {
	if (offset % int64(ab.BlockSize)) != 0 {
		log.Println("[ERROR] Stupid thing does not write on block boundary, offset:", offset, "length:", len(b))
	}
	conn := ab.Connections.Get()
	defer conn.Close()
	conn.Send("SET", offset, b)
	err = conn.Flush()
	if err == nil {
		bytesWritten = len(b)
	}
	return
}

//ReadAt implements nbd.Backend.ReadAt
func (ab *ArdbBackend) ReadAt(ctx context.Context, b []byte, offset int64) (bytesRead int, err error) {
	log.Println("Reading block at offset", offset, "len", len(b))
	offsetInsideBlock := offset % int64(ab.BlockSize)
	if offsetInsideBlock != 0 {
		offset -= offsetInsideBlock
	}

	conn := ab.Connections.Get()
	defer conn.Close()
	reply, err := conn.Do("GET", offset)
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
	//TODO: get the volume definition and set the size
	return 20000000000, 1, uint64(ab.BlockSize), 32 * 1024 * 1024, nil
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
	ab := &ArdbBackend{BlockSize: 4096}
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
