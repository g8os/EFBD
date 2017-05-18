package tlog

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/g8os/blockstor/config"
	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/redisstub"
	"github.com/garyburd/redigo/redis"
)

// AnyRedisPoolFactory creates any kind of RedisPoolFactory based on the given parameters.
// + if server configs are given, a static RedisPoolFactory will be created;
//   + if autoFill is true, any missing server configs will be
//     interfered based on the last given server Config (assuming sequential ports);
// + if no server configs are given but the configPath is set,
//   a RedisPoolFactory based on that config file will be created;
// + if no config path and no server configs are given,
//   an inmemory redis pool will be created instead;
func AnyRedisPoolFactory(cfg RedisPoolFactoryConfig) (RedisPoolFactory, error) {
	if length := len(cfg.ServerConfigs); length > 0 {
		// one extra (as we require also a metaDataServer)
		requiredServers := cfg.RequiredDataServerCount + 1

		serverConfigs := cfg.ServerConfigs
		if length < requiredServers {
			if !cfg.AutoFill {
				return nil, errors.New("not enough storage server configs are given")
			}

			var err error
			serverConfigs, err = autoFillStorageServerConfigs(requiredServers, serverConfigs)
			if err != nil {
				return nil, err
			}
		}

		// more of a dev option:
		// use the same predefined storage servers for all vdisks
		return StaticRedisPoolFactory(cfg.RequiredDataServerCount, serverConfigs)
	}

	// most desired option: config-based storage
	if cfg.ConfigPath != "" {
		return ConfigRedisPoolFactory(cfg.RequiredDataServerCount, cfg.ConfigPath)
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
// The first storage server is used metadata usage,
// and the rest of available servers are used as storage servers.
func StaticRedisPoolFactory(requiredDataServerCount int, storageServers []config.StorageServerConfig) (RedisPoolFactory, error) {
	redisPool, err := NewRedisPool(requiredDataServerCount, storageServers)
	if err != nil {
		return nil, err
	}

	return &staticRedisPoolFactory{redisPool}, nil
}

// ConfigRedisPoolFactory creates a RedisPoolFactory
// using a blockstor config file.
// The required amount of data servers is specified upfront,
// such that at creation of a RedisPool, it is validated that the
// storage cluster in question has sufficient data servers available.
func ConfigRedisPoolFactory(requiredDataServerCount int, configPath string) (RedisPoolFactory, error) {
	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't create RedisPoolFactory: %s", err.Error())
	}

	return &configRedisPoolFactory{
		cfg: cfg,
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
		// one extra (as we require also a metaDataServer)
		requiredServers := cfg.RequiredDataServerCount + 1

		serverConfigs := cfg.ServerConfigs
		if length < requiredServers {
			if !cfg.AutoFill {
				return nil, errors.New("not enough storage server configs are given")
			}

			var err error
			serverConfigs, err = autoFillStorageServerConfigs(requiredServers, serverConfigs)
			if err != nil {
				return nil, err
			}
		}

		// more of a dev option: use predefined storage servers
		return NewRedisPool(cfg.RequiredDataServerCount, serverConfigs)
	}

	// most desired option: config-based storage
	if cfg.ConfigPath != "" {
		return RedisPoolFromConfig(cfg.ConfigPath, cfg.VdiskID, cfg.RequiredDataServerCount)
	}

	// more of a test (or dev) option: simply use in-memory storage
	if cfg.AllowInMemory {
		return InMemoryRedisPool(cfg.RequiredDataServerCount), nil
	}

	return nil, errors.New("either a config path or server config has to be given")
}

// NewRedisPool creates a redis pool using the given servers,
// seperating the first one for metadata usage,
// and using the rest as storage servers.
func NewRedisPool(requiredDataServerCount int, storageServers []config.StorageServerConfig) (RedisPool, error) {
	if len(storageServers)-1 < requiredDataServerCount {
		return nil, errors.New("insufficient data servers given")
	}

	return newRedisPool(storageServers[0], storageServers[1:]), nil
}

// InMemoryRedisPool creates a redis pool using in-memory
// ardb storage servers.
func InMemoryRedisPool(requiredDataServerCount int) RedisPool {
	var serverConfigs []config.StorageServerConfig

	// one extra for metadata server
	requiredServers := requiredDataServerCount + 1

	for i := 0; i < requiredServers; i++ {
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

	return newRedisPool(serverConfigs[0], serverConfigs[1:])
}

// RedisPoolFromConfig creates a redis pool for a vdisk,
// using the storage cluster defined in the given Blokstor config file,
// for that vdisk.
func RedisPoolFromConfig(configPath, vdiskID string, requiredDataServerCount int) (RedisPool, error) {
	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		return nil, err
	}

	vdiskcfg, ok := cfg.Vdisks[vdiskID]
	if !ok {
		return nil, fmt.Errorf("no vdisk config could be found for %s", vdiskID)
	}

	if vdiskcfg.TlogStoragecluster == "" {
		return nil, fmt.Errorf("no tlog storage cluster defined for %s", vdiskID)
	}

	// should always be found, as this is validated during config loading
	storageCluster := cfg.StorageClusters[vdiskcfg.TlogStoragecluster]

	if len(storageCluster.DataStorage) < requiredDataServerCount {
		return nil, fmt.Errorf("storageCluster of vdisk %s has not enough dataservers", vdiskID)
	}

	// create redis pool based on the given valid storage cluster
	return newRedisPool(
		storageCluster.MetaDataStorage,
		storageCluster.DataStorage[1:],
	), nil
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

	// When ConfigPath is set, and no ServerConfigs have been specified,
	// the redis pool will be created using the config found on this path.
	ConfigPath string

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

	// When ConfigPath is set, and no ServerConfigs have been specified,
	// the redis pool factory will be created using the config found on this path.
	ConfigPath string

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
}

// RedisPool maintains a collection of premade redis.Pool's,
// one pool per predefined (meta)data connection.
type RedisPool interface {
	// returns amount of DataConnection servers available
	DataConnectionCount() int
	// The application calls the DataConnection with a valid index
	// to get a connection from the pool of data servers.
	// It is important that the caller of this function closes the
	// received connection to return the connection's resources back to the pool.
	DataConnection(index int) redis.Conn
	// The application calls the MetadataConnection
	// to get a connection from the metadataServer pool.
	// It is important that the caller of this function closes the
	// received connection to return the connection's resources back to the pool.
	MetadataConnection() redis.Conn
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

type configRedisPoolFactory struct {
	cfg                     *config.Config
	requiredDataServerCount int
}

// NewRedisPool implements RedisPoolFactory.NewRedisPool
func (factory *configRedisPoolFactory) NewRedisPool(vdiskID string) (RedisPool, error) {
	vdiskcfg, ok := factory.cfg.Vdisks[vdiskID]
	if !ok {
		return nil, fmt.Errorf("no vdisk config could be found for %s", vdiskID)
	}

	if vdiskcfg.TlogStoragecluster == "" {
		return nil, fmt.Errorf("no tlog storage cluster defined for %s", vdiskID)
	}

	// should always be found, as this is validated during config loading
	storageCluster := factory.cfg.StorageClusters[vdiskcfg.TlogStoragecluster]

	if len(storageCluster.DataStorage) < factory.requiredDataServerCount {
		return nil, fmt.Errorf("storageCluster of vdisk %s has not enough dataservers", vdiskID)
	}

	// create redis pool based on the given valid storage cluster
	return newRedisPool(
		storageCluster.MetaDataStorage,
		storageCluster.DataStorage[:factory.requiredDataServerCount-1],
	), nil
}

// redisPool is the internal structure used as the RedisPool
// in development/production code.
type redisPool struct {
	lock         sync.Mutex //protects following
	dataPools    map[int]*redis.Pool
	metaDataPool *redis.Pool
}

// newRedisPool creates a new pool for multiple redis servers,
// the number of pools defined by the number of given connection strings.
func newRedisPool(metadataServer config.StorageServerConfig, dataServers []config.StorageServerConfig) RedisPool {
	dataPools := make(map[int]*redis.Pool, len(dataServers))
	for index, dataServerConfig := range dataServers {
		dataPools[index] = newConnectionPool(dataServerConfig)
	}

	return &redisPool{
		dataPools:    dataPools,
		metaDataPool: newConnectionPool(metadataServer),
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

// MetadataConnection implements RedisPool.MetadataConnection
func (p *redisPool) MetadataConnection() redis.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.metaDataPool.Get()
}

// DataConnectionCount implements RedisPool.DataConnectionCount
func (p *redisPool) DataConnectionCount() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	return len(p.dataPools)
}

// DataConnection implements RedisPool.DataConnection
func (p *redisPool) DataConnection(index int) redis.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	if pool, ok := p.dataPools[index]; ok {
		return pool.Get()
	}

	return errorConnection{
		err: fmt.Errorf("no connectionPool exists for index %d", index),
	}
}

// Close implements RedisPool.Close
func (p *redisPool) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, c := range p.dataPools {
		c.Close()
	}
	p.metaDataPool.Close()

	p.dataPools, p.metaDataPool = nil, nil
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
