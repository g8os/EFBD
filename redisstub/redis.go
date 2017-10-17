// Package redisstub is a minimal package
// providing redis-related (in-memory) implementations
// meant for testing and dev purposes only.
package redisstub

import (
	"errors"

	"github.com/zero-os/0-Disk/redisstub/ledisdb"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
)

// NewMemoryRedis creates a new in-memory redis stub.
// It must be noted that the stub only partially redis-compliant,
// not all commands (such as MULTI/EXEC) are supported.
// All available commands can be found at:
// https://github.com/siddontang/ledisdb/blob/master/doc/commands.md
// WARNING: should be used for testing/dev purposes only!
func NewMemoryRedis() *MemoryRedis {
	return &MemoryRedis{
		server: ledisdb.NewServer(),
	}
}

// MemoryRedis is an in memory redis connection implementation
type MemoryRedis struct {
	server *ledisdb.Server
}

// Dial to the embedded Go Redis Server,
// and return the established connection if possible.
func (mr *MemoryRedis) Dial(_ string, _ int) (redis.Conn, error) {
	if mr == nil {
		return nil, errors.New("no in-memory redis is available")
	}

	return redis.Dial("tcp", mr.server.Address())
}

// Close the embedded Go Redis Server,
// and delete the used datadir.
func (mr *MemoryRedis) Close() {
	if mr == nil {
		return
	}
	mr.server.Close()
}

// address returns the tcp (local) address of this MemoryRedis server
func (mr *MemoryRedis) address() string {
	if mr == nil {
		return ""
	}

	return mr.server.Address()
}

// StorageServerConfig returns a new StorageServerConfig,
// usable to connect to this in-memory redis-compatible ledisdb.
func (mr *MemoryRedis) StorageServerConfig() config.StorageServerConfig {
	return config.StorageServerConfig{Address: mr.address()}
}

// NewMemoryRedisSlice creates a slice of in-memory redis stubs.
func NewMemoryRedisSlice(n int) *MemoryRedisSlice {
	if n <= 0 {
		panic("invalid memory redis slice count")
	}

	var slice []*MemoryRedis
	for i := 0; i < n; i++ {
		slice = append(slice, NewMemoryRedis())
	}
	return &MemoryRedisSlice{slice: slice}
}

// MemoryRedisSlice is a slice of in memory redis connection implementations
type MemoryRedisSlice struct {
	slice []*MemoryRedis
}

// StorageClusterConfig returns a new StorageClusterConfig,
// usable to connect to this slice of in-memory redis-compatible ledisdb.
func (mrs *MemoryRedisSlice) StorageClusterConfig() config.StorageClusterConfig {
	var cfg config.StorageClusterConfig
	if mrs == nil || mrs.slice == nil {
		return cfg
	}

	for _, server := range mrs.slice {
		cfg.Servers = append(cfg.Servers,
			config.StorageServerConfig{Address: server.address()})
	}
	return cfg
}

// CloseServer closes a specific server
func (mrs *MemoryRedisSlice) CloseServer(index int64) {
	mrs.slice[index].Close()
}

// Close implements ConnProvider.Close
func (mrs *MemoryRedisSlice) Close() error {
	for _, memRedis := range mrs.slice {
		memRedis.Close()
	}
	return nil
}
