package ardb

import (
	"errors"

	"github.com/zero-os/0-Disk/config"
)

// StorageCluster defines the interface of an
// object which allows you to interact with an ARDB Storage Cluster.
type StorageCluster interface {
	// Do applies a given action
	// to the first available server of this cluster.
	Do(action StorageAction) (reply interface{}, err error)
	// DoFor applies a given action
	// to a server within this cluster which maps to the given objectIndex.
	DoFor(objectIndex int64, action StorageAction) (reply interface{}, err error)
}

// NewUniCluster creates a new (ARDB) uni-cluster.
// See `UniCluster` for more information.
//   ErrNoServersAvailable is returned in case the given server isn't available.
//   ErrServerStateNotSupported is returned in case at the server
// has a state other than StorageServerStateOnline and StorageServerStateRIP.
func NewUniCluster(cfg config.StorageServerConfig, dialer ConnectionDialer) (*UniCluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	switch cfg.State {
	case config.StorageServerStateOnline:
		// OK
	case config.StorageServerStateRIP:
		return nil, ErrNoServersAvailable
	default:
		return nil, ErrServerStateNotSupported
	}

	if dialer == nil {
		dialer = stdConnDialer
	}

	return &UniCluster{
		server: cfg,
		dialer: dialer,
	}, nil
}

// UniCluster defines an in memory cluster model for a uni-cluster.
// Meaning it is a cluster with just /one/ ARDB server configured.
// As a consequence all methods will dial connections to this one server.
// This cluster type should only very be used for very specialized purposes.
type UniCluster struct {
	server config.StorageServerConfig
	dialer ConnectionDialer
}

// Do implements StorageCluster.Do
func (cluster *UniCluster) Do(action StorageAction) (interface{}, error) {
	// establish a connection for that serverIndex
	conn, err := cluster.dialer.Dial(cluster.server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// DoFor implements StorageCluster.DoFor
func (cluster *UniCluster) DoFor(_ int64, action StorageAction) (interface{}, error) {
	return cluster.Do(action)
}

// NewCluster creates a new (ARDB) cluster.
// ErrNoServersAvailable is returned in case no given server is available.
// ErrServerStateNotSupported is returned in case at least one server
// has a state other than StorageServerStateOnline and StorageServerStateRIP.
func NewCluster(cfg config.StorageClusterConfig, dialer ConnectionDialer) (*Cluster, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var clusterOperational bool
	for _, server := range cfg.Servers {
		if server.State == config.StorageServerStateOnline {
			clusterOperational = true
			continue
		}
		if server.State != config.StorageServerStateRIP {
			return nil, ErrServerStateNotSupported
		}
	}
	if !clusterOperational {
		return nil, ErrNoServersAvailable
	}

	if dialer == nil {
		dialer = stdConnDialer
	}

	return &Cluster{
		servers:     cfg.Servers,
		serverCount: int64(len(cfg.Servers)),
		dialer:      dialer,
	}, nil
}

// Cluster defines the in memory cluster model for a single cluster.
type Cluster struct {
	servers     []config.StorageServerConfig
	serverCount int64

	dialer ConnectionDialer
}

// Do implements StorageCluster.Do
func (cluster *Cluster) Do(action StorageAction) (interface{}, error) {
	// compute server index of first available server
	serverIndex, err := FindFirstServerIndex(cluster.serverCount, cluster.serverOperational)
	if err != nil {
		return nil, err
	}
	return cluster.doAt(serverIndex, action)
}

// DoFor implements StorageCluster.DoFor
func (cluster *Cluster) DoFor(objectIndex int64, action StorageAction) (interface{}, error) {
	// compute server index which maps to the given object index
	serverIndex, err := ComputeServerIndex(cluster.serverCount, objectIndex, cluster.serverOperational)
	if err != nil {
		return nil, err
	}
	return cluster.doAt(serverIndex, action)
}

func (cluster *Cluster) doAt(serverIndex int64, action StorageAction) (interface{}, error) {
	// establish a connection for that serverIndex
	conn, err := cluster.dialer.Dial(cluster.servers[serverIndex])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// apply the given action to the established connection
	return action.Do(conn)
}

// serverOperational returns true if
// the server on the given index is operational.
func (cluster *Cluster) serverOperational(index int64) (bool, error) {
	// Because of the constructor,
	// we know for sure that the state is either online or RIP.
	ok := cluster.servers[index].State == config.StorageServerStateOnline
	return ok, nil
}

// NopCluster is a Cluster which can be used for
// scenarios where you want to specify a StorageCluster,
// which only ever returns NoServersAvailable error.
type NopCluster struct{}

// Do implements StorageCluster.Do
func (cluster NopCluster) Do(action StorageAction) (reply interface{}, err error) {
	return nil, ErrNoServersAvailable
}

// DoFor implements StorageCluster.DoFor
func (cluster NopCluster) DoFor(objectIndex int64, action StorageAction) (reply interface{}, err error) {
	return nil, ErrNoServersAvailable
}

// ServerIndexPredicate is a predicate
// used to determine if a given serverIndex
// for a callee-owned cluster object is valid.
// An error can be returned to quit early in a search.
type ServerIndexPredicate func(serverIndex int64) (bool, error)

// FindFirstAvailableServerConfig iterates through all storage servers
// until it finds a server which state indicates its available.
// If no such server exists, ErrNoServersAvailable is returned.
// TODO: delete this function,
//       and instead force primitives to work with clusters,
//       as otherwise we might start to diviate in decision-making,
//       depending on whether this funcion is used or the static cluster type.
// see: https://github.com/zero-os/0-Disk/issues/549
func FindFirstAvailableServerConfig(cfg config.StorageClusterConfig) (serverCfg config.StorageServerConfig, err error) {
	for _, serverCfg = range cfg.Servers {
		if serverCfg.State == config.StorageServerStateOnline {
			return
		}
	}

	err = ErrNoServersAvailable
	return
}

// FindFirstServerIndex iterates through all servers
// until the predicate for a server index returns true.
// If no index evaluates to true, ErrNoServersAvailable is returned.
func FindFirstServerIndex(serverCount int64, predicate ServerIndexPredicate) (serverIndex int64, err error) {
	var ok bool
	for ; serverIndex < serverCount; serverIndex++ {
		ok, err = predicate(serverIndex)
		if ok || err != nil {
			return
		}
	}

	return -1, ErrNoServersAvailable
}

// ComputeServerIndex computes a server index for a given objectIndex,
// using a shared static algorithm with the serverCount as input and
// a given predicate to define if a computed index is fine.
// NOTE: the callee has to ensure that at least /one/ server is operational!
func ComputeServerIndex(serverCount, objectIndex int64, predicate ServerIndexPredicate) (serverIndex int64, err error) {
	// first try the modulo sharding,
	// which will work for all default online shards
	// and thus keep it as cheap as possible
	serverIndex = objectIndex % serverCount
	ok, err := predicate(serverIndex)
	if ok || err != nil {
		return
	}

	// keep trying until we find an online server
	// in the same reproducable manner
	// (another kind of tracing)
	// using jumpConsistentHash taken from https://arxiv.org/pdf/1406.2294.pdf
	var j int64
	var key uint64
	for {
		key = uint64(objectIndex)
		j = 0
		for j < serverCount {
			serverIndex = j
			key = key*2862933555777941757 + 1
			j = int64(float64(serverIndex+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
		}
		ok, err = predicate(serverIndex)
		if ok || err != nil {
			return
		}

		objectIndex++
	}
}

var (
	stdConnDialer = new(StandardConnectionDialer)
)

// enforces that our std StorageClusters
// are actually StorageClusters
var (
	_ StorageCluster = (*UniCluster)(nil)
	_ StorageCluster = (*Cluster)(nil)
	_ StorageCluster = NopCluster{}
)

var (
	// ErrServerUnavailable is returned in case
	// a given server is unavailable (e.g. offline).
	ErrServerUnavailable = errors.New("server is unavailable")

	// ErrNoServersAvailable is returned in case
	// no servers are available for usage.
	ErrNoServersAvailable = errors.New("no servers available")

	// ErrServerIndexOOB is returned in case
	// a given server index used is out of bounds,
	// for the cluster context it is used within.
	ErrServerIndexOOB = errors.New("server index out of bounds")

	// ErrInvalidInput is an error returned
	// when the input given for a function is invalid.
	ErrInvalidInput = errors.New("invalid input given")

	// ErrServerStateNotSupported is an error ereturned
	// when a server is updated to a state,
	// while the cluster (type) does not support that kind of state.
	ErrServerStateNotSupported = errors.New("server state is not supported")
)
