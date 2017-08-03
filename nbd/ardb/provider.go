package ardb

import (
	"context"
	"errors"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// ConnProvider defines the interface to get ARDB connections
// to (meta)data storage servers.
type ConnProvider interface {
	DataConnProvider
	MetadataConnProvider

	Close() error
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

// StaticProvider creates a Static Provider using the given NBD Config.
func StaticProvider(cfg config.NBDStorageConfig, pool *RedisPool) (ConnProvider, error) {
	if pool == nil {
		pool = NewRedisPool(nil)
	}

	provider := &staticRedisProvider{redisPool: pool}
	provider.setConfig(&cfg)
	return provider, nil
}

// DynamicProvider creates a provider which always
// has the most up to date config it can know about.
func DynamicProvider(ctx context.Context, vdiskID string, source config.Source, pool *RedisPool) (ConnProvider, error) {
	if pool == nil {
		pool = NewRedisPool(nil)
	}

	provider := &redisProvider{
		static: staticRedisProvider{redisPool: pool},
		done:   make(chan struct{}),
	}

	// Start listen goroutine, which gives the initial config (if successfull,
	// as well as provide the future updates of that config.
	// If the config could not be fetch initially, an error will be returned instead.
	err := provider.listen(ctx, vdiskID, source)
	if err != nil {
		return nil, err
	}

	return provider, nil
}

type staticRedisProvider struct {
	redisPool *RedisPool

	// used to get a redis connection
	dataConnectionConfigs []config.StorageServerConfig
	numberOfServers       int64 //Keep it as a seperate variable since this is constantly needed

	// used for getting template data from a template server
	templateDataConnectionConfigs []config.StorageServerConfig
	numberOfTemplateServers       int64 //Keep it as a seperate variable since this is constantly needed

	// used to store meta data
	metaConnectionConfig *config.StorageServerConfig
}

// DataConnection implements DataConnProvider.DataConnection
func (rp *staticRedisProvider) DataConnection(index int64) (conn redis.Conn, err error) {
	bcIndex := index % rp.numberOfServers
	connConfig := &rp.dataConnectionConfigs[bcIndex]

	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// TemplateConnection implements DataConnProvider.TemplateConnection
func (rp *staticRedisProvider) TemplateConnection(index int64) (conn redis.Conn, err error) {
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
func (rp *staticRedisProvider) MetadataConnection() (conn redis.Conn, err error) {
	connConfig := rp.metaConnectionConfig
	conn = rp.redisPool.Get(connConfig.Address, connConfig.Database)
	return
}

// Close implements ConnProvider.Close
func (rp *staticRedisProvider) Close() error {
	rp.redisPool.Close()
	return nil
}

func (rp *staticRedisProvider) setConfig(cfg *config.NBDStorageConfig) {
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
}

// redisProvider allows you to get a redis connection from a pool
// using a modulo index
type redisProvider struct {
	// used to contain the actual storage information,
	// and dispatch all connection logic to this static type
	static staticRedisProvider

	// used to close background listener
	done chan struct{}
	// to protect connection configs
	mux sync.RWMutex
}

// DataConnection implements DataConnProvider.DataConnection
func (rp *redisProvider) DataConnection(index int64) (redis.Conn, error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()
	return rp.static.DataConnection(index)
}

// TemplateConnection implements DataConnProvider.TemplateConnection
func (rp *redisProvider) TemplateConnection(index int64) (redis.Conn, error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()
	return rp.static.TemplateConnection(index)
}

// MetadataConnection implements MetadataConnProvider.MetaConnection
func (rp *redisProvider) MetadataConnection() (redis.Conn, error) {
	rp.mux.RLock()
	defer rp.mux.RUnlock()
	return rp.static.MetadataConnection()
}

// spawns listen goroutine which gives the initial config (if successfull,
// as well as provide the future updates of that config.
// If the config could not be fetch initially, an error will be returned instead.
func (rp *redisProvider) listen(ctx context.Context, vdiskID string, source config.Source) error {
	ctx, cancelFunc := context.WithCancel(ctx)

	log.Debug("create nbd config listener for ", vdiskID)
	ch, err := config.WatchNBDStorageConfig(ctx, source, vdiskID)
	if err != nil {
		cancelFunc()
		return err
	}

	// get the initial NBD config
	cfg := <-ch
	rp.static.setConfig(&cfg)

	log.Debug("spawn redisProvider listener goroutine for ", vdiskID)
	go func() {
		defer log.Debug("exit redisProvider listener from ", vdiskID)
		defer cancelFunc()

		for {
			select {
			case cfg := <-ch:
				rp.static.setConfig(&cfg)

			case <-rp.done:
				rp.done = nil
				return

			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Close the internal redis pool and stop the listen goroutine.
func (rp *redisProvider) Close() error {
	if rp.done == nil {
		return nil
	}

	close(rp.done)
	return rp.static.Close()
}

var (
	// error returned when no template storage cluster has been defined,
	// while a connection to it was requested.
	errNoTemplateAvailable = errors.New("no template ardb connection available")
)
