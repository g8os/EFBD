package tlog

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/redisstub"
)

// AnyRedisPoolFactory creates any kind of RedisPoolFactory based on the given parameters.
// + if server configs are given, a static RedisPoolFactory will be created;
//   + if autoFill is true, any missing server configs will be
//     interfered based on the last given server Config (assuming sequential ports);
// + if no server configs are given but the configPath is set,
//   a RedisPoolFactory based on that config file will be created;
// + if no config path and no server configs are given,
//   an inmemory redis pool will be created instead;
func AnyRedisPoolFactory(ctx context.Context, cfg RedisPoolFactoryConfig) (RedisPoolFactory, error) {
	if length := len(cfg.ServerConfigs); length > 0 {
		serverConfigs := cfg.ServerConfigs
		if length < cfg.RequiredDataServerCount {
			if !cfg.AutoFill {
				return nil, errors.New("not enough storage server configs are given")
			}

			var err error
			serverConfigs, err = autoFillStorageServerConfigs(cfg.RequiredDataServerCount, serverConfigs)
			if err != nil {
				return nil, err
			}
		}

		// more of a dev option:
		// use the same predefined storage servers for all vdisks
		return StaticRedisPoolFactory(cfg.RequiredDataServerCount, serverConfigs)
	}

	// most desired option: config-based storage
	if err := cfg.ConfigInfo.Validate(); err == nil {
		return ConfigRedisPoolFactory(ctx, cfg.ConfigInfo, cfg.RequiredDataServerCount)
	}

	// final resort: inmemory storage
	return InMemoryRedisPoolFactory(cfg.RequiredDataServerCount), nil
}

// InMemoryRedisPoolFactory creates a static RedisPoolFactory,
// which returns the same valid inmemory RedisPool for all vdisks,
// and thus it will always return a valid RedisPool.
func InMemoryRedisPoolFactory(requiredDataServerCount int) RedisPoolFactory {
	return &staticRedisPoolFactory{
		redisPool: InMemoryRedisPool(requiredDataServerCount),
	}
}

// StaticRedisPoolFactory creates a RedisPoolFactory,
// which returns the same valid RedisPool for all vdisks,
// and thus it will always return a valid RedisPool.
func StaticRedisPoolFactory(requiredDataServerCount int, storageServers []config.StorageServerConfig) (RedisPoolFactory, error) {
	redisPool, err := NewRedisPool(requiredDataServerCount, storageServers)
	if err != nil {
		return nil, err
	}

	return &staticRedisPoolFactory{redisPool}, nil
}

// ConfigRedisPoolFactory creates a RedisPoolFactory
// using a zerodisk config file.
// The required amount of data servers is specified upfront,
// such that at creation of a RedisPool, it is validated that the
// storage cluster in question has sufficient data servers available.
func ConfigRedisPoolFactory(ctx context.Context, configInfo zerodisk.ConfigInfo, requiredDataServerCount int) (RedisPoolFactory, error) {
	return &configRedisPoolFactory{
		ctx:                     ctx,
		configInfo:              configInfo,
		requiredDataServerCount: requiredDataServerCount,
	}, nil
}

// AnyRedisPool creates any kind of RedisPool based on the given parameters.
// + if server configs are given, a RedisPool based on those will be created;
//   + if autoFill is true, any missing server configs will be
//     interfered based on the last given server Config (assuming sequential ports);
// + if no server configs are given but the configPath is set,
//   a RedisPool will be created from that config file;
// + if no config path and no server configs are given,
//   and allowInMemory is true, an inmemory redis pool will be created instead;
func AnyRedisPool(cfg RedisPoolConfig) (RedisPool, error) {
	if length := len(cfg.ServerConfigs); length > 0 {
		serverConfigs := cfg.ServerConfigs
		if length < cfg.RequiredDataServerCount {
			if !cfg.AutoFill {
				return nil, errors.New("not enough storage server configs are given")
			}

			var err error
			serverConfigs, err = autoFillStorageServerConfigs(cfg.RequiredDataServerCount, serverConfigs)
			if err != nil {
				return nil, err
			}
		}

		// more of a dev option: use predefined storage servers
		return NewRedisPool(cfg.RequiredDataServerCount, serverConfigs)
	}

	// most desired option: config-based storage
	if err := cfg.ConfigInfo.Validate(); err == nil {
		return RedisPoolFromConfig(cfg.VdiskID, cfg.ConfigInfo, cfg.RequiredDataServerCount)
	}

	// more of a test (or dev) option: simply use in-memory storage
	if cfg.AllowInMemory {
		return InMemoryRedisPool(cfg.RequiredDataServerCount), nil
	}

	return nil, errors.New("either a config path or server config has to be given")
}

// NewRedisPool creates a redis pool using the given servers.
func NewRedisPool(requiredDataServerCount int, storageServers []config.StorageServerConfig) (RedisPool, error) {
	if len(storageServers) < requiredDataServerCount {
		return nil, errors.New("insufficient data servers given")
	}

	return newStaticRedisPool(storageServers), nil
}

// InMemoryRedisPool creates a redis pool using in-memory
// ardb storage servers.
func InMemoryRedisPool(requiredDataServerCount int) RedisPool {
	var serverConfigs []config.StorageServerConfig

	for i := 0; i < requiredDataServerCount; i++ {
		memoryRedis := redisstub.NewMemoryRedis()
		memoryAddress := memoryRedis.Address()

		go func() {
			defer func() {
				log.Debug("closing inmemory storage server: ", memoryAddress)
				memoryRedis.Close()
			}()
			memoryRedis.Listen()
		}()

		serverConfigs = append(serverConfigs, config.StorageServerConfig{
			Address:  memoryAddress,
			Database: 0,
		})
	}

	return newStaticRedisPool(serverConfigs)
}

// RedisPoolFromConfig creates a redis pool for a vdisk,
// using the storage cluster defined in the given Blokstor config file,
// for that vdisk.
func RedisPoolFromConfig(vdiskID string, configInfo zerodisk.ConfigInfo, requiredDataServerCount int) (RedisPool, error) {
	cfg, err := zerodisk.ReadTlogConfig(vdiskID, configInfo)
	if err != nil {
		return nil, err
	}

	if len(cfg.TlogStorageCluster.DataStorage) < requiredDataServerCount {
		return nil, fmt.Errorf(
			"storageCluster of vdisk %s has not enough dataservers",
			vdiskID)
	}

	// create redis pool based on the given valid storage cluster
	return newStaticRedisPool(cfg.TlogStorageCluster.DataStorage), nil
}

// RedisPoolConfig used to create any kind
// of supported redis pool,
// based on the properties set in this config.
// Note that this config should only be used because the choice is made
// by input given by the user in one way or another.
// If the choice is known on compile time, use the correct constructor instead.
type RedisPoolConfig struct {
	// RequiredDataServerCount is required by all types of
	// redis pools, in order to validate or create the right amount
	// of storage servers.
	RequiredDataServerCount int

	// When ConfigInfo is set and valid, and no ServerConfigs have been specified,
	// the config will be read from the specified config source.
	ConfigInfo zerodisk.ConfigInfo

	// The vdisk ID to use,
	// only required in case the config path is given.
	VdiskID string

	// Create a redis pool straight from these server configs,
	// the redis pool requires RequiredDataServerCount+1 configs to be specified
	ServerConfigs []config.StorageServerConfig
	// AutoFill allows you to automatically complete the server config slice,
	// in case not enough server configs (RequiredDataServerCount+1) have been given.
	// Auto filling is done, by assuming that all missing servers are on
	// the same IP address of the last given server config, using sequential ports,
	// following the port of that last given server config.
	AutoFill bool

	// Enable this boolean, in case you want to allow an in-memory redis pool.
	AllowInMemory bool
}

// RedisPoolFactoryConfig used to create any kind
// of supported redis pool factory,
// based on the properties set in this config.
// Note that this config should only be used because the choice is made
// by input given by the user in one way or another.
// If the choice is known on compile time, use the correct constructor instead.
type RedisPoolFactoryConfig struct {
	// RequiredDataServerCount is required by all types of
	// redis pool factories, in order to validate or create the right amount
	// of storage servers.
	RequiredDataServerCount int

	// When ConfigResource is set (valid), and no ServerConfigs have been specified,
	// the redis pool factory will be created using the specified config info,
	// and thus the created pools will read configs from the specified config source.
	ConfigInfo zerodisk.ConfigInfo

	// Create a redis pool factory straight from these server configs,
	// the redis pool factor) requires RequiredDataServerCount+1 configs to be specified
	ServerConfigs []config.StorageServerConfig
	// AutoFill allows you to automatically complete the server config slice,
	// in case not enough server configs (RequiredDataServerCount+1) have been given.
	// Auto filling is done, by assuming that all missing servers are on
	// the same IP address of the last given server config, using sequential ports,
	// following the port of that last given server config.
	AutoFill bool

	// Enable this boolean, in case you want to allow an in-memory redis pool (factory).
	AllowInMemory bool
}

// RedisPoolFactory creates a redis pool for a given vdisk.
type RedisPoolFactory interface {
	// NewRedisPool creates a new redis pool for the given vdisk.
	// An error is returned in case that vdisk couldn't be found,
	// or in case that vdisk has not enough (n < k+m) dataservers linked to it,
	// in order to create a valid RedisPool.
	NewRedisPool(vdiskID string) (RedisPool, error)
	// Close any open resources
	Close() error
}

// RedisPool maintains a collection of premade redis.Pool's,
type RedisPool interface {
	// returns amount of DataConnection servers available
	DataConnectionCount() int
	// The application calls the DataConnection with a valid index
	// to get a connection from the pool of data servers.
	// It is important that the caller of this function closes the
	// received connection to return the connection's resources back to the pool.
	DataConnection(index int) redis.Conn
	// Close all open resources.
	Close()
}

type staticRedisPoolFactory struct {
	redisPool RedisPool
}

// NewRedisPool implements RedisPoolFactory.NewRedisPool
func (factory *staticRedisPoolFactory) NewRedisPool(string) (RedisPool, error) {
	return factory.redisPool, nil
}

// Close implements RedisPoolFactory.Close
func (factory *staticRedisPoolFactory) Close() error { return nil }

type configRedisPoolFactory struct {
	ctx                     context.Context
	configInfo              zerodisk.ConfigInfo
	requiredDataServerCount int
}

// NewRedisPool implements RedisPoolFactory.NewRedisPool
func (factory *configRedisPoolFactory) NewRedisPool(vdiskID string) (RedisPool, error) {
	return newDynamicRedisPool(
		factory.ctx,
		factory.configInfo,
		vdiskID,
		factory.requiredDataServerCount,
	)
}

// Close implements RedisPoolFactory.Close
func (factory *configRedisPoolFactory) Close() error {
	return nil
}

// staticRedisPool is the static version used as the RedisPool
// in development/production code.
type staticRedisPool struct {
	lock      sync.RWMutex //protects following
	dataPools map[int]*redis.Pool
}

// newStaticRedisPool creates a new pool for multiple redis servers,
// the number of pools defined by the number of given connection strings.
func newStaticRedisPool(dataServers []config.StorageServerConfig) RedisPool {
	dataPools := make(map[int]*redis.Pool, len(dataServers))
	for index, dataServerConfig := range dataServers {
		dataPools[index] = newConnectionPool(dataServerConfig)
	}

	return &staticRedisPool{
		dataPools: dataPools,
	}
}

// newConnectionPool creates a connection based on a given
func newConnectionPool(cfg config.StorageServerConfig) *redis.Pool {
	return &redis.Pool{
		MaxActive:   3,
		MaxIdle:     3,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", cfg.Address, redis.DialDatabase(cfg.Database))
		},
	}
}

// DataConnectionCount implements RedisPool.DataConnectionCount
func (p *staticRedisPool) DataConnectionCount() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.dataPools)
}

// DataConnection implements RedisPool.DataConnection
func (p *staticRedisPool) DataConnection(index int) redis.Conn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if pool, ok := p.dataPools[index]; ok {
		return pool.Get()
	}

	return errorConnection{
		err: fmt.Errorf("no connectionPool exists for index %d", index),
	}
}

// Close implements RedisPool.Close
func (p *staticRedisPool) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, c := range p.dataPools {
		c.Close()
	}

	p.dataPools = nil
}

// newDynamicRedisPool creates a new pool for multiple redis servers,
// the number of pools defined by the number of given connection strings.
func newDynamicRedisPool(ctx context.Context, configInfo zerodisk.ConfigInfo, vdiskID string, requiredNumberOfDataPools int) (RedisPool, error) {
	redisPool := &dynamicRedisPool{
		requiredNumberOfDataPools: requiredNumberOfDataPools,
		dataPools:                 make(map[int]*redisPool),
		done:                      make(chan struct{}),
	}

	// start background thread (for config updates)
	err := redisPool.listen(ctx, configInfo, vdiskID)
	if err != nil {
		return nil, err
	}

	// return redisPool, has to be closed by user
	return redisPool, nil
}

// dynamicRedisPool is the dynamic version used as the RedisPool
// in development/production code.
type dynamicRedisPool struct {
	lock sync.RWMutex

	requiredNumberOfDataPools int
	dataPools                 map[int]*redisPool

	done chan struct{}
}

// redisPool combines the redis.Pool,
// together with the information it used to create it
type redisPool struct {
	*redis.Pool
	Config config.StorageServerConfig
}

// DataConnectionCount implements RedisPool.DataConnectionCount
func (p *dynamicRedisPool) DataConnectionCount() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.requiredNumberOfDataPools
}

// DataConnection implements RedisPool.DataConnection
func (p *dynamicRedisPool) DataConnection(index int) redis.Conn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if pool, ok := p.dataPools[index]; ok {
		return pool.Get()
	}

	return errorConnection{
		err: fmt.Errorf("no connectionPool exists for index %d", index),
	}
}

// Close implements RedisPool.Close
func (p *dynamicRedisPool) Close() {
	if p.done == nil {
		return
	}

	close(p.done)

	p.lock.Lock()
	defer p.lock.Unlock()

	for _, c := range p.dataPools {
		c.Close()
	}
}

func (p *dynamicRedisPool) reloadConfig(cfg *config.TlogConfig) error {
	numServers := len(cfg.TlogStorageCluster.DataStorage)
	if numServers < p.requiredNumberOfDataPools {
		return errors.New("not enough tlog servers defined")
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	log.Debug("(re)loading redis pools, using the latest available config")

	configs := cfg.TlogStorageCluster.DataStorage[:p.requiredNumberOfDataPools]
	for index, cfg := range configs {
		if pool, ok := p.dataPools[index]; ok {
			// pool already exists, so let's see
			// if we can keep it as it is
			if pool.Config == cfg {
				continue // pool is fine as it is
			}

			log.Debugf("closing replaced pool #%d: %v", index, &pool.Config)
			p.dataPools[index].Close()
		}

		p.dataPools[index] = &redisPool{
			Pool:   newConnectionPool(cfg),
			Config: cfg,
		}
	}

	return nil
}

func (p *dynamicRedisPool) listen(ctx context.Context, configInfo zerodisk.ConfigInfo, vdiskID string) error {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	log.Debug("creating tlog config watch for ", vdiskID)
	ch, err := zerodisk.WatchTlogConfig(ctx, vdiskID, configInfo)
	if err != nil {
		return err
	}

	log.Debug("use initial tlog config from ", vdiskID)
	// get initial config,
	// which should be given if the watch was created correctly.
	cfg := <-ch
	if err = p.reloadConfig(&cfg); err != nil {
		return err
	}

	log.Debug("listen (async) for incoming tlog config updates for ", vdiskID)
	go func() {
		for {
			select {
			case cfg := <-ch:
				err := p.reloadConfig(&cfg)
				if err != nil {
					log.Error(err)
				}

			case <-p.done:
				p.done = nil
				return
			}
		}
	}()

	return nil
}

// errorConnection taken from
// https://github.com/garyburd/redigo/blob/ac91d6ff49bd0d278a90201de77a4f8ad9628e25/redis/pool.go#L409-L416
type errorConnection struct{ err error }

func (ec errorConnection) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConnection) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConnection) Err() error                                     { return ec.err }
func (ec errorConnection) Close() error                                   { return ec.err }
func (ec errorConnection) Flush() error                                   { return ec.err }
func (ec errorConnection) Receive() (interface{}, error)                  { return nil, ec.err }

// adds any missing storage server configs based on the last given storage server config
func autoFillStorageServerConfigs(requiredServers int, serverConfigs []config.StorageServerConfig) ([]config.StorageServerConfig, error) {
	length := len(serverConfigs)
	if length >= requiredServers {
		return serverConfigs[:requiredServers-1], nil
	}

	lastAddr := serverConfigs[length-1].Address
	host, rawPort, err := net.SplitHostPort(lastAddr)
	if err != nil {
		return nil, fmt.Errorf("storage address %s is illegal: %v", lastAddr, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return nil, fmt.Errorf("storage address %s is illegal: %v", lastAddr, err)
	}
	// add the missing servers dynamically
	// (it is assumed that the missing servers are live on the ports
	//  following the last server's port)
	port++
	portBound := port + (requiredServers - length)
	for ; port < portBound; port++ {
		addr := net.JoinHostPort(host, strconv.Itoa(port))
		log.Debug("add missing objstor server address:", addr)
		serverConfigs = append(serverConfigs, config.StorageServerConfig{
			Address:  addr,
			Database: 0,
		})
	}

	return serverConfigs, nil
}
