package redisstub

import (
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// NewUniCluster creates a new (LedisDB) uni-cluster.
// See `UniCluster` for more information.
func NewUniCluster(pooled bool) *UniCluster {
	mr := NewMemoryRedis()

	var cluster *ardb.UniCluster
	var pool *ardb.Pool
	var err error

	if pooled {
		pool = ardb.NewPool(nil)
		cluster, err = ardb.NewUniCluster(mr.StorageServerConfig(), pool)
	} else {
		cluster, err = ardb.NewUniCluster(mr.StorageServerConfig(), nil)
	}
	if err != nil {
		pool.Close()
		panic(err)
	}

	return &UniCluster{
		UniCluster: cluster,
		mr:         mr,
		pool:       pool,
	}
}

// UniCluster defines an in memory cluster model for an in-memory (ledisdb) uni-cluster.
type UniCluster struct {
	*ardb.UniCluster
	mr   *MemoryRedis
	pool *ardb.Pool
}

// StorageServerConfig returns a new StorageServerConfig,
// usable to connect to this in-memory redis-compatible ledisdb.
func (cluster *UniCluster) StorageServerConfig() config.StorageServerConfig {
	return cluster.mr.StorageServerConfig()
}

// Close all open resources.
func (cluster *UniCluster) Close() {
	if cluster == nil {
		return
	}

	cluster.mr.Close()
	if cluster.pool != nil {
		cluster.pool.Close()
	}
}

// NewCluster creates a new (LedisDB) cluster.
// See `Cluster` for more information.
func NewCluster(n int, pooled bool) *Cluster {
	mrs := NewMemoryRedisSlice(n)

	var cluster *ardb.Cluster
	var pool *ardb.Pool
	var err error

	if pooled {
		pool = ardb.NewPool(nil)
		cluster, err = ardb.NewCluster(mrs.StorageClusterConfig(), pool)
	} else {
		cluster, err = ardb.NewCluster(mrs.StorageClusterConfig(), nil)
	}
	if err != nil {
		pool.Close()
		panic(err)
	}

	return &Cluster{
		Cluster: cluster,
		mrs:     mrs,
		pool:    pool,
	}
}

// Cluster defines an in memory cluster model for an in-memory (ledisdb) cluster.
type Cluster struct {
	*ardb.Cluster
	mrs  *MemoryRedisSlice
	pool *ardb.Pool
}

// StorageClusterConfig returns a new StorageClusterConfig,
// usable to connect to this slice of in-memory redis-compatible ledisdb.
func (cluster *Cluster) StorageClusterConfig() config.StorageClusterConfig {
	return cluster.mrs.StorageClusterConfig()
}

// Close all open resources.
func (cluster *Cluster) Close() {
	if cluster == nil {
		return
	}

	cluster.mrs.Close()
	if cluster.pool != nil {
		cluster.pool.Close()
	}
}
