package ardb

import (
	"context"
	"errors"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/siddontang/go/log"
	"github.com/zero-os/0-Disk/config"
)

// ConnProvider defines the interface to get ARDB connections
// to (meta)data storage servers.
type ConnProvider interface {
	DataConnProvider
	MetadataConnProvider

	Close() error
}

// ConnProviderHotReloader defines the interface for a ConnProvider
// which also supports hotreloading of the underlying configuration,
// meaning the returned server information will always be up todate.
type ConnProviderHotReloader interface {
	ConnProvider

	Listen(ctx context.Context, vdiskID string, ch <-chan config.NBDConfig)
}

// DataConnProvider defines the interface to get an ARDB data connection,
// based on a given index, used by the arbd storage backends
type DataConnProvider interface {
	// DataConnection gets an ardb (data) connection based on the given modulo index,
	// which depends on the available servers in the cluster used by the provider.
	DataConnection(index int64) (conn redis.Conn, err error)
	// TemplateConnection gets an ardb (template data) connection based on the given modulo index,
	// which depends on the available servers in the cluster used by the provider.
	TemplateConnection(index int64) (conn redis.Conn, err error)
}

// MetadataConnProvider defines the interface to get an ARDB metadata connection.
type MetadataConnProvider interface {
	// MetaConnection gets an ardb (metadata) connection
	// the the metadata storage server of the cluster used by the provider.
	MetadataConnection() (conn redis.Conn, err error)
}

// RedisProvider creates a Provider using Redis.
func RedisProvider(cfg *config.NBDConfig, pool *RedisPool) (ConnProviderHotReloader, error) {
	if pool == nil {
		pool = NewRedisPool(nil)
	}

	provider := &redisProvider{
		redisPool: pool,
		done:      make(chan struct{}),
	}

	// load nbdserver config
	err := provider.reloadConfig(cfg)
	if err != nil {
		return nil, err
	}

	return provider, nil
}

// redisProvider allows you to get a redis connection from a pool
// using a modulo index
type redisProvider struct {
	redisPool *RedisPool

	done chan struct{}

	// used to get a redis connection
	dataConnectionConfigs []config.StorageServerConfig
	numberOfServers       int64 //Keep it as a seperate variable since this is constantly needed

	// used for getting template data from a template server
	templateDataConnectionConfigs []config.StorageServerConfig
	numberOfTemplateServers       int64 //Keep it as a seperate variable since this is constantly needed

	// used to store meta data
	metaConnectionConfig *config.StorageServerConfig

	// to protect connection configs
	mux sync.RWMutex
}

// DataConnection implements DataConnProvider.DataConnection
func (rp *redisProvider) DataConnection(index int64) (conn redis.Conn, err error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()

	bcIndex := index % rp.numberOfServers
	connConfig := &rp.dataConnectionConfigs[bcIndex]

	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// TemplateConnection implements DataConnProvider.TemplateConnection
func (rp *redisProvider) TemplateConnection(index int64) (conn redis.Conn, err error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()

	// not all vdisks have a templateStoragecluster defined,
	// it is therefore not a guarantee that at least one server is available,
	// a given we do have in the ConnectionString method
	if rp.numberOfTemplateServers < 1 {
		err = errNoTemplateAvailable
		return
	}

	bcIndex := index % rp.numberOfTemplateServers
	connConfig := &rp.templateDataConnectionConfigs[bcIndex]

	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// MetadataConnection implements MetadataConnProvider.MetaConnection
func (rp *redisProvider) MetadataConnection() (conn redis.Conn, err error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()

	connConfig := rp.metaConnectionConfig
	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// Listen to the config hot reloader,
// and reload the nbdserver config incase it has been updated.
// TODO: who closes this channel?
//   |---> I guess we should share the ctx with the one given to this listener?!
func (rp *redisProvider) Listen(ctx context.Context, vdiskID string, ch <-chan config.NBDConfig) {
	log.Debug("redisProvider listening")

	defer log.Debug("exit redisProvider listener")

	for {
		select {
		case cfg := <-ch:
			err := rp.reloadConfig(&cfg)
			if err != nil {
				log.Error(err)
			}

		case <-rp.done:
			return

		case <-ctx.Done():
			return
		}
	}
}

// Close the internal redis pool and stop the listen goroutine.
func (rp *redisProvider) Close() error {
	if rp.done == nil {
		return nil
	}

	close(rp.done)
	rp.done = nil
	rp.redisPool.Close()
	return nil
}

// Update all internally stored parameters we care about,
// based on the updated config content.
func (rp *redisProvider) reloadConfig(cfg *config.NBDConfig) error {
	rp.mux.Lock()
	defer rp.mux.Unlock()

	rp.dataConnectionConfigs = cfg.StorageCluster.DataStorage
	rp.numberOfServers = int64(len(rp.dataConnectionConfigs))

	rp.metaConnectionConfig = cfg.StorageCluster.MetadataStorage

	if cfg.TemplateStorageCluster == nil {
		rp.templateDataConnectionConfigs = nil
		rp.numberOfTemplateServers = 0
	} else {
		rp.templateDataConnectionConfigs = cfg.TemplateStorageCluster.DataStorage
		rp.numberOfTemplateServers = int64(len(rp.templateDataConnectionConfigs))
	}

	return nil
}

var (
	// error returned when no template storage cluster has been defined,
	// while a connection to it was requested.
	errNoTemplateAvailable = errors.New("no template ardb connection available")
)
