package slavesync

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

// NewSlaveCluster creates a new SlaveCluster.
// See `SlaveCluster` for more information.
func NewSlaveCluster(ctx context.Context, vdiskID string, cs config.Source) (*SlaveCluster, error) {
	slaveCluster := &SlaveCluster{
		vdiskID: vdiskID,
		pool:    ardb.NewPool(nil),
	}
	err := slaveCluster.spawnConfigReloader(ctx, cs)
	if err != nil {
		slaveCluster.Close()
		return nil, err
	}

	return slaveCluster, nil
}

// SlaveCluster defines a vdisk's slave cluster.
// It supports hot reloading of the configuration
// and state handling of the individual servers of a cluster.
type SlaveCluster struct {
	vdiskID string

	servers     []config.StorageServerConfig
	serverCount int64

	pool   *ardb.Pool
	cancel context.CancelFunc

	mux sync.RWMutex
}

// Do implements StorageCluster.Do
func (sc *SlaveCluster) Do(action ardb.StorageAction) (reply interface{}, err error) {
	sc.mux.RLock()

	// slave cluster is optional, so could be not defined
	// returning an error is required though,
	// as a slave custer is required where we do use this type of cluster.
	if sc.serverCount == 0 {
		sc.mux.RUnlock()
		return nil, storage.ErrClusterNotDefined
	}

	// compute server index of first available server
	serverIndex, err := ardb.FindFirstServerIndex(sc.serverCount, sc.serverOperational)
	if err != nil {
		sc.mux.RUnlock()
		return nil, err
	}
	cfg := sc.servers[serverIndex]
	sc.mux.RUnlock()

	return sc.doAt(serverIndex, cfg, action)
}

// DoFor implements StorageCluster.DoFor
func (sc *SlaveCluster) DoFor(objectIndex int64, action ardb.StorageAction) (reply interface{}, err error) {
	sc.mux.RLock()

	// slave cluster is optional, so could be not defined
	// returning an error is required though,
	// as a slave custer is required where we do use this type of cluster.
	if sc.serverCount == 0 {
		sc.mux.RUnlock()
		return nil, storage.ErrClusterNotDefined
	}

	// compute server index for the server which maps to the given object index
	serverIndex, err := ardb.ComputeServerIndex(sc.serverCount, objectIndex, sc.serverOperational)
	if err != nil {
		sc.mux.RUnlock()
		return nil, err
	}
	cfg := sc.servers[serverIndex]
	sc.mux.RUnlock()

	return sc.doAt(serverIndex, cfg, action)
}

// DoForAll implements StorageCluster.DoForAll
func (sc *SlaveCluster) DoForAll(pairs []ardb.IndexActionPair) ([]interface{}, error) {
	// a shortcut in case we have received no pairs, or just a single one
	switch len(pairs) {
	case 0:
		return nil, nil // nothing to do
	case 1:
		reply, err := sc.DoFor(pairs[0].Index, pairs[1].Action)
		if err != nil {
			return nil, err
		}
		return []interface{}{reply}, nil
	}

	sc.mux.RLock()
	defer sc.mux.RUnlock()

	// sort all actions in terms of their mapped server index
	servers := make(map[int64]*ardb.IndexActionMap)
	for index, pair := range pairs {
		// compute server index which maps to the given object index
		serverIndex, err := ardb.ComputeServerIndex(sc.serverCount, pair.Index, sc.serverOperational)
		if err != nil {
			return nil, err
		}
		// add the pair to the relevant map
		server, ok := servers[serverIndex]
		if !ok {
			server = new(ardb.IndexActionMap)
			servers[serverIndex] = server
		}
		server.Add(int64(index), pair.Action)
	}

	// a shortcut if we are lucky enough to only have 1 server
	if len(servers) == 1 {
		for serverIndex, indexActionMap := range servers {
			return ardb.Values(sc.doAt(serverIndex, sc.servers[serverIndex],
				ardb.Commands(indexActionMap.Actions...)))
		}
	}

	// create result -type and -channel to collect all replies async
	type serverResult struct {
		ServerIndex int64
		Replies     []interface{}
		Error       error
	}
	ch := make(chan serverResult)

	// apply all actions async, and collect the results

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for serverIndex := range servers {
		wg.Add(1)

		// local variables to be used within the goroutine scope
		indexActionMap := servers[serverIndex]
		result := serverResult{ServerIndex: serverIndex}

		go func() {
			defer wg.Done()
			result.Replies, result.Error = ardb.Values(sc.doAt(
				result.ServerIndex, sc.servers[result.ServerIndex],
				ardb.Commands(indexActionMap.Actions...)))
			select {
			case ch <- result:
			case <-ctx.Done():
			}
		}()
	}

	// close channel when all inpu
	go func() {
		wg.Wait()
		close(ch)
	}()

	// we'll collect all replies in order
	replies := make([]interface{}, len(pairs))

	// collect all replies
	for result := range ch {
		// return early if an error occured
		if result.Error != nil {
			return nil, errors.Wrapf(result.Error,
				"error while applying actions in serverIndex %d", result.ServerIndex)
		}

		// get the indexActionMap for the given server,
		// such that we can retrieve the correct reply index
		m := servers[result.ServerIndex]

		// collect all received replies in order
		for i, reply := range result.Replies {
			replyIndex := m.Indices[i]
			replies[replyIndex] = reply
		}
	}

	// return all replies from all servers in ordered form
	return replies, nil
}

// ServerIterator implements StorageCluster.ServerIterator.
func (sc *SlaveCluster) ServerIterator(ctx context.Context) (<-chan ardb.StorageServer, error) {
	sc.mux.Lock()

	// slave cluster is optional, so could be not defined
	// returning an error is required though,
	// as a slave custer is required where we do use this type of cluster.
	if sc.serverCount == 0 {
		sc.mux.Unlock()
		return nil, storage.ErrClusterNotDefined
	}

	ch := make(chan ardb.StorageServer)
	go func() {
		// we only want to unlock this cluster for other operations,
		// when the server iterator has been fully consumed.
		defer sc.mux.Unlock()
		defer close(ch)

		for index := int64(0); index < sc.serverCount; index++ {
			operational, _ := sc.serverOperational(index)
			if !operational {
				continue
			}

			server := slaveStorageServer{
				index:   index,
				cluster: sc,
			}

			select {
			case <-ctx.Done():
				return
			case ch <- server:
			}
		}
	}()
	return ch, nil
}

// ServerCount implements StorageCluster.ServerCount.
func (sc *SlaveCluster) ServerCount() int64 {
	sc.mux.RLock()

	count := sc.serverCount
	for _, server := range sc.servers {
		if server.State != config.StorageServerStateOnline {
			count--
		}
	}
	sc.mux.RUnlock()

	return count
}

// execute an exuction at a given primary server
func (sc *SlaveCluster) doAt(serverIndex int64, cfg config.StorageServerConfig, action ardb.StorageAction) (reply interface{}, err error) {
	// establish a connection for the given config
	conn, err := sc.pool.Dial(cfg)
	if err == nil {
		defer conn.Close()
		reply, err = action.Do(conn)
		if err == nil || err == ardb.ErrNil {
			return
		}
	}

	// TODO:
	// add self-healing...
	// see: https://github.com/zero-os/0-Disk/issues/445
	// and  https://github.com/zero-os/0-Disk/issues/284

	// an error has occured, broadcast it to AYS
	status := storage.MapErrorToBroadcastStatus(err)
	log.Broadcast(
		status,
		log.SubjectStorage,
		log.ARDBServerTimeoutBody{
			Address:  cfg.Address,
			Database: cfg.Database,
			Type:     log.ARDBSlaveServer,
			VdiskID:  sc.vdiskID,
		},
	)

	// mark server as offline, so that next time this server will trigger an error,
	// such that we don't broadcast all the time
	if err := sc.updateServerState(serverIndex, config.StorageServerStateOffline); err != nil {
		log.Errorf("couldn't update slave server (%d) state to offline: %v", serverIndex, err)
	}

	return nil, ardb.ErrServerUnavailable
}

// Close any open resources
func (sc *SlaveCluster) Close() error {
	sc.cancel()
	sc.pool.Close()
	return nil
}

// serverOperational returns true if
// a server on the given index is available for operation.
func (sc *SlaveCluster) serverOperational(index int64) (bool, error) {
	switch sc.servers[index].State {
	case config.StorageServerStateOnline:
		return true, nil

	case config.StorageServerStateOffline:
		return false, ardb.ErrServerUnavailable

	case config.StorageServerStateRIP:
		return false, nil

	default:
		return false, ardb.ErrServerStateNotSupported
	}
}

func (sc *SlaveCluster) updateServerState(index int64, state config.StorageServerState) error {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	err := sc.handleServerStateUpdate(index, state)
	if err != nil {
		return err
	}

	log.Debugf("updating vdisk %s' slave server #%d state to %s", sc.vdiskID, index, state)
	sc.servers[index].State = state
	return nil
}

func (sc *SlaveCluster) handleServerStateUpdate(index int64, state config.StorageServerState) error {
	if sc.servers[index].State == state {
		return nil // nothing to do
	}

	// [TODO]
	// Handle Online => Nothing to do ?!
	// Handle Offline => Ignore Write calls; Returns Primary content for Reads
	// Handle Restore => Copy data from primary to slave, Mark server afterwards as Online
	// Handle Respread => Copy data from primary to slave, Mark server afterwards as RIP
	// Handle RIP => nothing to do, broadcast this even though, just to be sure AYS knows about this
	//
	// [TODO] should we also check the state flow? e.g. does `RIP => Online` make sense?!

	switch state {
	case config.StorageServerStateOnline, config.StorageServerStateOffline, config.StorageServerStateRIP:
		return nil // supported

	default:
		return ardb.ErrServerStateNotSupported
	}
}

// spawnConfigReloader starts all needed config watchers,
// and spawns a goroutine to receive the updates.
// An error is returned in case the initial watch-creation and config-update failed.
// All future errors will be logged (and optionally broadcasted),
// without stopping this goroutine.
func (sc *SlaveCluster) spawnConfigReloader(ctx context.Context, cs config.Source) error {
	// create the context and cancelFunc used for the master watcher.
	ctx, sc.cancel = context.WithCancel(ctx)

	// create the master watcher if possible
	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, sc.vdiskID)
	if err != nil {
		return err
	}
	vdiskNBDConfig := <-vdiskNBDRefCh

	var slaveClusterCfg config.StorageClusterConfig

	// create the slave storage cluster watcher,
	// and execute the initial config update iff
	// an internal watcher is created.
	var slaveWatcher storage.ClusterConfigWatcher
	clusterExists, err := slaveWatcher.SetClusterID(ctx, cs, sc.vdiskID, vdiskNBDConfig.SlaveStorageClusterID)
	if err != nil {
		return err
	}

	// slave cluster is optional,
	// so it's fine if the cluster doesn't exist yet at this point,
	// we'll create the cluster anyhow, in case it does start to exist.
	if clusterExists {
		slaveClusterCfg = <-slaveWatcher.Receive()
		err = sc.updateSlaveStorageConfig(slaveClusterCfg)
		if err != nil {
			return err
		}
	}

	// spawn the config update goroutine
	go func() {
		var ok bool
		for {
			select {
			case <-ctx.Done():
				return

			// handle clusterID reference updates
			case vdiskNBDConfig, ok = <-vdiskNBDRefCh:
				if !ok {
					return
				}

				_, err = slaveWatcher.SetClusterID(
					ctx, cs, sc.vdiskID, vdiskNBDConfig.SlaveStorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new slave cluster config: %v", err)
				}

			// handle slave cluster storage updates
			case slaveClusterCfg = <-slaveWatcher.Receive():
				err = sc.updateSlaveStorageConfig(slaveClusterCfg)
				if err != nil {
					log.Errorf("failed to update new slave cluster config: %v", err)
				}
			}
		}
	}()

	// all is operational, no error to return
	return nil
}

// updateSlaveStorageConfig overwrites
// the currently used slave storage config,
func (sc *SlaveCluster) updateSlaveStorageConfig(cfg config.StorageClusterConfig) error {
	sc.mux.Lock()
	defer sc.mux.Unlock()

	serverCount := int64(len(cfg.Servers))
	if serverCount > sc.serverCount {
		serverCount = sc.serverCount
	}

	var err error
	var origServer, newServer config.StorageServerConfig

	for index := int64(0); index < serverCount; index++ {
		origServer, newServer = sc.servers[index], cfg.Servers[index]
		if !storageServersEqual(origServer, newServer) {
			continue // a new server or non-changed state, so no update here
		}

		err = sc.handleServerStateUpdate(index, newServer.State)
		if err != nil {
			return err
		}
	}

	sc.servers = cfg.Servers
	sc.serverCount = int64(len(cfg.Servers))
	return nil
}

// slaveStorageServer defines a slave storage server.
type slaveStorageServer struct {
	index   int64
	cluster *SlaveCluster
}

// Do implements StorageServer.Do
func (server slaveStorageServer) Do(action ardb.StorageAction) (reply interface{}, err error) {
	cfg := server.cluster.servers[server.index]
	return server.cluster.doAt(server.index, cfg, action)
}

// Config implements StorageServer.Config
func (server slaveStorageServer) Config() config.StorageServerConfig {
	return server.cluster.servers[server.index]
}

// storageServersEqual compares if 2 storage server configs
// are equal, except for their state.
func storageServersEqual(a, b config.StorageServerConfig) bool {
	return a.Database == b.Database &&
		a.Address == b.Address
}

// enforces that our StorageClusters
// are actually StorageClusters
var (
	_ ardb.StorageCluster = (*SlaveCluster)(nil)
)

// enforces that our ServerIterators
// are actually ServerIterators
var (
	_ ardb.StorageServer = slaveStorageServer{}
)
