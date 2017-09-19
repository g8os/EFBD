package ardb

import (
	"context"
	"errors"
	"sync"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

var (
	// ErrServerMarkedInvalid is returned in case a server is not valid,
	// and has been marked as such.
	ErrServerMarkedInvalid = errors.New("ardb server marked invalid")
	// ErrTemplateClusterNotSpecified is returned in case a
	// template server is not specified for a vdisk,
	// while a server for it is reuqired.
	ErrTemplateClusterNotSpecified = errors.New("template cluster not configured for vdisk")
)

// Connection defines the interface expected for any ARDB connection.
type Connection interface {
	redis.Conn

	// ConnectionConfig returns the configuration used to
	// configure this connection.
	ConnectionConfig() config.StorageServerConfig
}

// newConnection returns a new available ARDB connection from a given pool,
// using a given storage server config.
func newConnection(pool *RedisPool, cfg config.StorageServerConfig) (*connection, error) {
	if cfg.Disabled {
		return nil, errDisabledStorageServer
	}

	conn := pool.Get(cfg.Address, cfg.Database)
	return &connection{
		Conn: conn,
		cfg:  cfg,
	}, nil
}

// connection is a simple struct, which combines an ARDB connection,
// with its configuration it was created with.
type connection struct {
	redis.Conn
	cfg config.StorageServerConfig
}

// ConnectionConfig implements Connection.ConnectionConfig
func (conn *connection) ConnectionConfig() config.StorageServerConfig {
	return conn.cfg
}

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
	DataConnection(index int64) (conn Connection, err error)
	// TemplateConnection gets an ardb (template data) connection based on the given modulo index,
	// which depends on the available servers in the cluster used by the provider.
	TemplateConnection(index int64) (conn Connection, err error)
	// MarkTemplateConnectionInvalid marks the server for a given index invalid,
	// which is to be used as a hint that a connection for that server no longer has to be returned,
	// until the connection issue has been resolved.
	MarkTemplateConnectionInvalid(index int64)
}

// MetadataConnProvider defines the interface to get an ARDB metadata connection.
type MetadataConnProvider interface {
	// MetaConnection gets an ardb (metadata) connection
	// the the metadata storage server of the cluster used by the provider.
	MetadataConnection() (conn Connection, err error)
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
}

// DataConnection implements DataConnProvider.DataConnection
func (rp *staticRedisProvider) DataConnection(index int64) (conn Connection, err error) {
	// first try the modulo sharding,
	// which will work for all default online shards
	// and thus keep it as cheap as possible
	bcIndex := index % rp.numberOfServers
	connConfig := rp.dataConnectionConfigs[bcIndex]
	if !connConfig.Disabled {
		conn, err = newConnection(rp.redisPool, connConfig)
		return
	}

	// keep trying until we find a non-offline shard
	// in the same reproducable manner
	// (another kind of tracing)
	// using jumpConsistentHash taken from https://arxiv.org/pdf/1406.2294.pdf
	var j int64
	var key uint64
	for {
		key = uint64(index)
		j = 0
		for j < rp.numberOfServers {
			bcIndex = j
			key = key*2862933555777941757 + 1
			j = int64(float64(bcIndex+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
		}
		connConfig = rp.dataConnectionConfigs[bcIndex]
		if !connConfig.Disabled {
			conn, err = newConnection(rp.redisPool, connConfig)
			return
		}

		index++
	}
}

// TemplateConnection implements DataConnProvider.TemplateConnection
func (rp *staticRedisProvider) TemplateConnection(index int64) (conn Connection, err error) {
	// not all vdisks have a templateStoragecluster defined,
	// it is therefore not a guarantee that at least one server is available,
	// a given we do have in the ConnectionString method
	if rp.numberOfTemplateServers < 1 {
		err = ErrTemplateClusterNotSpecified
		return
	}

	bcIndex := index % rp.numberOfTemplateServers
	connConfig := rp.templateDataConnectionConfigs[bcIndex]

	// if a config address is not given,
	// it can only be because the server has been marked invalid by the user of this provider.
	if connConfig.Address == "" {
		return nil, ErrServerMarkedInvalid
	}

	conn, err = newConnection(rp.redisPool, connConfig)
	return
}

// MarkTemplateConnectionInvalid implements DataConnProvider.MarkTemplateConnectionInvalid
func (rp *staticRedisProvider) MarkTemplateConnectionInvalid(index int64) {
	if rp.numberOfTemplateServers < 1 {
		return
	}

	// disable the template server for the given index,
	// until the next config reload or until the end.
	bcIndex := index % rp.numberOfTemplateServers
	rp.templateDataConnectionConfigs[bcIndex].Address = ""
}

// MetadataConnection implements MetadataConnProvider.MetaConnection
func (rp *staticRedisProvider) MetadataConnection() (conn Connection, err error) {
	for _, connConfig := range rp.dataConnectionConfigs {
		conn, err = newConnection(rp.redisPool, connConfig)
		if err == nil {
			return
		}
	}

	return nil, errNoStorageServerAvailable
}

// Close implements ConnProvider.Close
func (rp *staticRedisProvider) Close() error {
	rp.redisPool.Close()
	return nil
}

func (rp *staticRedisProvider) setConfig(cfg *config.NBDStorageConfig) {
	rp.dataConnectionConfigs = cfg.StorageCluster.DataStorage
	rp.numberOfServers = int64(len(rp.dataConnectionConfigs))

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
	dataMux     sync.RWMutex
	metaMux     sync.RWMutex
	templateMux sync.RWMutex
}

// DataConnection implements DataConnProvider.DataConnection
func (rp *redisProvider) DataConnection(index int64) (Connection, error) {
	rp.dataMux.RLock()
	defer rp.dataMux.RUnlock()
	return rp.static.DataConnection(index)
}

// TemplateConnection implements DataConnProvider.TemplateConnection
func (rp *redisProvider) TemplateConnection(index int64) (Connection, error) {
	rp.templateMux.RLock()
	defer rp.templateMux.RUnlock()
	return rp.static.TemplateConnection(index)
}

// MarkTemplateConnectionInvalid implements DataConnProvider.MarkTemplateConnectionInvalid
func (rp *redisProvider) MarkTemplateConnectionInvalid(index int64) {
	rp.templateMux.Lock()
	defer rp.templateMux.Unlock()
	rp.static.MarkTemplateConnectionInvalid(index)
}

// MetadataConnection implements MetadataConnProvider.MetaConnection
func (rp *redisProvider) MetadataConnection() (Connection, error) {
	rp.metaMux.RLock()
	defer rp.metaMux.RUnlock()
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
				rp.dataMux.Lock()
				rp.metaMux.Lock()
				rp.templateMux.Lock()
				rp.static.setConfig(&cfg)
				rp.dataMux.Unlock()
				rp.metaMux.Unlock()
				rp.templateMux.Unlock()

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
	close(rp.done)
	return rp.static.Close()
}

var (
	errDisabledStorageServer    = errors.New("storage server is disabled")
	errNoStorageServerAvailable = errors.New("no storage server available")
)
