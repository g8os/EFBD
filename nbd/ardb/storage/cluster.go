package storage

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// TODO:
// add server states (and stub handling)
// https://github.com/zero-os/0-Disk/issues/455

// NewPrimaryCluster creates a new PrimaryCluster.
// See `PrimaryCluster` for more information.
func NewPrimaryCluster(ctx context.Context, vdiskID string, cs config.Source) (*PrimaryCluster, error) {
	primaryCluster := &PrimaryCluster{
		vdiskID: vdiskID,
		pool:    ardb.NewPool(nil),
	}
	err := primaryCluster.spawnConfigReloader(ctx, cs)
	if err != nil {
		primaryCluster.Close()
		return nil, err
	}

	return primaryCluster, nil
}

// PrimaryCluster defines a vdisk's primary cluster.
// It supports hot reloading of the configuration
// and state handling of the individual servers of a cluster.
type PrimaryCluster struct {
	vdiskID string

	servers     []config.StorageServerConfig
	serverCount int64

	pool   *ardb.Pool
	cancel context.CancelFunc

	mux sync.RWMutex
}

// Do implements StorageCluster.Do
func (pc *PrimaryCluster) Do(action ardb.StorageAction) (reply interface{}, err error) {
	pc.mux.RLock()
	// compute server index of first available server
	serverIndex, err := ardb.FindFirstServerIndex(pc.serverCount, pc.serverOperational)
	if err != nil {
		pc.mux.RUnlock()
		return nil, err
	}
	cfg := pc.servers[serverIndex]
	pc.mux.RUnlock()

	return pc.doAt(serverIndex, cfg, action)
}

// DoFor implements StorageCluster.DoFor
func (pc *PrimaryCluster) DoFor(objectIndex int64, action ardb.StorageAction) (reply interface{}, err error) {
	pc.mux.RLock()
	// compute server index for the server which maps to the given object index
	serverIndex, err := ardb.ComputeServerIndex(pc.serverCount, objectIndex, pc.serverOperational)
	if err != nil {
		pc.mux.RUnlock()
		return nil, err
	}
	cfg := pc.servers[serverIndex]
	pc.mux.RUnlock()

	return pc.doAt(serverIndex, cfg, action)
}

// ServerIterator implements StorageCluster.ServerIterator.
// The returned ServerIterator has be used within a single goroutine!
func (pc *PrimaryCluster) ServerIterator() (ardb.ServerIterator, error) {
	pc.mux.RLock()
	serverIndex, err := ardb.FindFirstServerIndex(pc.serverCount, pc.serverOperational)
	pc.mux.RUnlock()
	if err != nil {
		return nil, err
	}

	return &primaryClusterServerIterator{
		cluster: pc,
		cursor:  serverIndex - 1,
	}, nil
}

// execute an exuction at a given primary server
func (pc *PrimaryCluster) doAt(serverIndex int64, cfg config.StorageServerConfig, action ardb.StorageAction) (reply interface{}, err error) {
	// establish a connection for the given config
	conn, err := pc.pool.Dial(cfg)
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
	status := mapErrorToBroadcastStatus(err)
	log.Broadcast(
		status,
		log.SubjectStorage,
		log.ARDBServerTimeoutBody{
			Address:  cfg.Address,
			Database: cfg.Database,
			Type:     log.ARDBPrimaryServer,
			VdiskID:  pc.vdiskID,
		},
	)

	// mark server as offline, so that next time this server will trigger an error,
	// such that we don't broadcast all the time
	if err := pc.updateServerState(serverIndex, config.StorageServerStateOffline); err != nil {
		log.Errorf("couldn't update primary server (%d) state to offline: %v", serverIndex, err)
	}

	return nil, ardb.ErrServerUnavailable
}

// Close any open resources
func (pc *PrimaryCluster) Close() error {
	pc.cancel()
	pc.pool.Close()
	return nil
}

// serverOperational returns true if
// a server on the given index is available for operation.
func (pc *PrimaryCluster) serverOperational(index int64) (bool, error) {
	switch pc.servers[index].State {
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

func (pc *PrimaryCluster) updateServerState(index int64, state config.StorageServerState) error {
	pc.mux.Lock()
	defer pc.mux.Unlock()

	err := pc.handleServerStateUpdate(index, state)
	if err != nil {
		return err
	}

	log.Debugf("updating vdisk %s' primary server #%d state to %s", pc.vdiskID, index, state)
	pc.servers[index].State = state
	return nil
}

func (pc *PrimaryCluster) handleServerStateUpdate(index int64, state config.StorageServerState) error {
	if pc.servers[index].State == state {
		return nil // nothing to do
	}

	// [TODO]
	// Handle Online => Nothing to do ?!
	// Handle Offline => Notify Tlog server to not sync with TLog
	// Handle Restore => Copy data from slave to primary, Notify TLog server it can re-use that slave
	// Handle Respread => Copy data from slave to primary, Mark server afterwards as RIP
	// Handle RIP => nothing to do (should tlog be warned of this though?!)
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
func (pc *PrimaryCluster) spawnConfigReloader(ctx context.Context, cs config.Source) error {
	// create the context and cancelFunc used for the master watcher.
	ctx, pc.cancel = context.WithCancel(ctx)

	// create the master watcher if possible
	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, pc.vdiskID)
	if err != nil {
		return err
	}
	vdiskNBDConfig := <-vdiskNBDRefCh

	var primaryClusterCfg config.StorageClusterConfig

	// create the primary storage cluster watcher,
	// and execute the initial config update iff
	// an internal watcher is created.
	var primaryWatcher storageClusterWatcher
	clusterExists, err := primaryWatcher.SetClusterID(ctx, cs, pc.vdiskID, vdiskNBDConfig.StorageClusterID)
	if err != nil {
		return err
	}
	if !clusterExists {
		panic("primary cluster should exist on a non-error path")
	}
	primaryClusterCfg = <-primaryWatcher.Receive()
	err = pc.updatePrimaryStorageConfig(primaryClusterCfg)
	if err != nil {
		return err
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

				_, err = primaryWatcher.SetClusterID(
					ctx, cs, pc.vdiskID, vdiskNBDConfig.StorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new primary cluster config: %v", err)
				}

			// handle primary cluster storage updates
			case primaryClusterCfg = <-primaryWatcher.Receive():
				err = pc.updatePrimaryStorageConfig(primaryClusterCfg)
				if err != nil {
					log.Errorf("failed to update new primary cluster config: %v", err)
				}
			}
		}
	}()

	// all is operational, no error to return
	return nil
}

// updatePrimaryStorageConfig overwrites
// the currently used primary storage config,
func (pc *PrimaryCluster) updatePrimaryStorageConfig(cfg config.StorageClusterConfig) error {
	pc.mux.Lock()
	defer pc.mux.Unlock()

	serverCount := int64(len(cfg.Servers))
	if serverCount > pc.serverCount {
		serverCount = pc.serverCount
	}

	var err error
	var origServer, newServer config.StorageServerConfig

	for index := int64(0); index < serverCount; index++ {
		origServer, newServer = pc.servers[index], cfg.Servers[index]
		if !storageServersEqual(origServer, newServer) {
			continue // a new server or non-changed state, so no update here
		}

		err = pc.handleServerStateUpdate(index, newServer.State)
		if err != nil {
			return err
		}
	}

	pc.servers = cfg.Servers
	pc.serverCount = int64(len(cfg.Servers))
	return nil
}

// primaryClusterServerIterator implement a server iterator,
// for a primary cluster with multiple servers.
type primaryClusterServerIterator struct {
	cluster *PrimaryCluster
	cursor  int64
}

// Do implements ServerIterator.Do
func (it *primaryClusterServerIterator) Do(action ardb.StorageAction) (reply interface{}, err error) {
	it.cluster.mux.RLock()
	cfg := it.cluster.servers[it.cursor]
	it.cluster.mux.RUnlock()
	return it.cluster.doAt(it.cursor, cfg, action)
}

// Next implements ServerIterator.Next
func (it *primaryClusterServerIterator) Next() bool {
	it.cluster.mux.RLock()
	defer it.cluster.mux.RUnlock()

	if it.cursor >= it.cluster.serverCount {
		return false
	}

	var ok bool
	for it.cursor++; it.cursor < it.cluster.serverCount; it.cursor++ {
		if ok, _ = it.cluster.serverOperational(it.cursor); ok {
			return true
		}
	}

	return true
}

// NewTemplateCluster creates a new TemplateCluster.
// See `TemplateCluster` for more information.
func NewTemplateCluster(ctx context.Context, vdiskID string, cs config.Source) (*TemplateCluster, error) {
	templateCluster := &TemplateCluster{
		vdiskID: vdiskID,
		pool:    ardb.NewPool(nil),
	}
	err := templateCluster.spawnConfigReloader(ctx, cs)
	if err != nil {
		templateCluster.Close()
		return nil, err
	}

	return templateCluster, nil
}

// TemplateCluster defines a vdisk'stemplate cluster (configured or not).
// It supports hot reloading of the configuration.
type TemplateCluster struct {
	vdiskID string

	servers     []config.StorageServerConfig
	serverCount int64

	pool   *ardb.Pool
	cancel context.CancelFunc

	mux sync.RWMutex
}

// Do implements StorageCluster.Do
func (tsc *TemplateCluster) Do(_ ardb.StorageAction) (reply interface{}, err error) {
	return nil, ErrMethodNotSupported
}

// DoFor implements StorageCluster.DoFor
func (tsc *TemplateCluster) DoFor(objectIndex int64, action ardb.StorageAction) (reply interface{}, err error) {
	tsc.mux.RLock()
	cfg, serverIndex, err := tsc.serverConfigFor(objectIndex)
	tsc.mux.RUnlock()
	if err != nil {
		return nil, err
	}
	return tsc.doAt(serverIndex, cfg, action)
}

// ServerIterator implements StorageCluster.ServerIterator.
// The returned ServerIterator has be used within a single goroutine!
func (tsc *TemplateCluster) ServerIterator() (ardb.ServerIterator, error) {
	tsc.mux.RLock()
	defer tsc.mux.RUnlock()

	if tsc.serverCount == 0 {
		return nil, ErrClusterNotDefined
	}

	serverIndex, err := ardb.FindFirstServerIndex(tsc.serverCount, tsc.serverOperational)
	if err != nil {
		return nil, err
	}

	return &templateClusterServerIterator{
		cluster: tsc,
		cursor:  serverIndex - 1,
	}, nil
}

// Close any open resources
func (tsc *TemplateCluster) Close() error {
	tsc.cancel()
	tsc.pool.Close()
	return nil
}

func (tsc *TemplateCluster) doAt(serverIndex int64, cfg config.StorageServerConfig, action ardb.StorageAction) (reply interface{}, err error) {
	conn, err := tsc.pool.Dial(cfg)
	if err == nil {
		defer conn.Close()
		reply, err = action.Do(conn)
		if err == nil || err == ardb.ErrNil {
			return
		}
	}

	// an error has occured, broadcast it to AYS
	status := mapErrorToBroadcastStatus(err)
	log.Broadcast(
		status,
		log.SubjectStorage,
		log.ARDBServerTimeoutBody{
			Address:  cfg.Address,
			Database: cfg.Database,
			Type:     log.ARDBTemplateServer,
			VdiskID:  tsc.vdiskID,
		},
	)

	tsc.mux.Lock()
	updateErr := tsc.updateServerState(serverIndex, config.StorageServerStateOffline)
	tsc.mux.Unlock()
	if updateErr != nil {
		log.Errorf("couldn't update template server (%d) state to offline: %v", serverIndex, updateErr)
	}

	return nil, ardb.ErrServerUnavailable
}

// spawnConfigReloader starts all needed config watchers,
// and spawns a goroutine to receive the updates.
// An error is returned in case the initial watch-creation and config-update failed.
// All future errors will be logged (and optionally broadcasted),
// without stopping this goroutine.
func (tsc *TemplateCluster) spawnConfigReloader(ctx context.Context, cs config.Source) error {
	// create the context and cancelFunc used for the master watcher.
	ctx, tsc.cancel = context.WithCancel(ctx)

	// create the master watcher if possible
	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, tsc.vdiskID)
	if err != nil {
		return err
	}
	vdiskNBDConfig := <-vdiskNBDRefCh

	// create the storage cluster watcher,
	// and execute the initial config update iff
	// an internal watcher is created.
	var watcher storageClusterWatcher
	clusterExists, err := watcher.SetClusterID(
		ctx, cs, tsc.vdiskID, vdiskNBDConfig.TemplateStorageClusterID)
	if err != nil {
		return err
	}
	var templateClusterCfg config.StorageClusterConfig
	if clusterExists {
		templateClusterCfg = <-watcher.Receive()
		err = tsc.updateStorageConfig(templateClusterCfg)
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

				clusterWasDefined := watcher.Defined()
				clusterExists, err = watcher.SetClusterID(
					ctx, cs, tsc.vdiskID, vdiskNBDConfig.TemplateStorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new template cluster config: %v", err)
					continue
				}
				if clusterWasDefined && !clusterExists {
					// no cluster exists any longer, we need to delete the old state
					tsc.mux.Lock()
					tsc.servers, tsc.serverCount = nil, 0
					tsc.mux.Unlock()
				}

			// handle cluster storage updates
			case templateClusterCfg = <-watcher.Receive():
				err = tsc.updateStorageConfig(templateClusterCfg)
				if err != nil {
					log.Errorf("failed to update new template cluster config: %v", err)
				}
			}
		}
	}()

	// all is operational, no error to return
	return nil
}

// updateStorageConfig overwrites the currently used storage config,
// iff the given config is valid.
func (tsc *TemplateCluster) updateStorageConfig(cfg config.StorageClusterConfig) error {
	var clusterOperational bool
	for _, server := range cfg.Servers {
		if server.State == config.StorageServerStateOnline {
			clusterOperational = true
			break
		}
	}
	if !clusterOperational {
		// no servers are available,
		// so no need to use the config at all
		tsc.mux.Lock()
		tsc.servers, tsc.serverCount = nil, 0
		tsc.mux.Unlock()
		return nil
	}

	tsc.mux.Lock()
	tsc.servers = cfg.Servers
	tsc.serverCount = int64(len(cfg.Servers))
	tsc.mux.Unlock()
	return nil
}

func (tsc *TemplateCluster) serverConfigFor(objectIndex int64) (cfg config.StorageServerConfig, serverIndex int64, err error) {
	// ensure the template cluster is actually defined,
	// as it is created even when no clusterID is referenced,
	// just in case one would be defined via a hotreload.
	if tsc.serverCount == 0 {
		err = ErrClusterNotDefined
		return
	}

	// compute server index for the server which maps to the given object index
	serverIndex, err = ardb.ComputeServerIndex(tsc.serverCount, objectIndex, tsc.serverOperational)
	if err != nil {
		return
	}

	// establish a connection for that serverIndex
	cfg = tsc.servers[serverIndex]
	return
}

func (tsc *TemplateCluster) updateServerState(index int64, state config.StorageServerState) error {
	switch state {
	case config.StorageServerStateOnline, config.StorageServerStateOffline, config.StorageServerStateRIP:

		log.Debugf("updating vdisk %s' template server #%d state to %s", tsc.vdiskID, index, state)
		tsc.servers[index].State = state
		return nil

	default:
		return ardb.ErrServerStateNotSupported
	}
}

// serverOperational returns true if
// a server on the given index is available for operation.
func (tsc *TemplateCluster) serverOperational(index int64) (bool, error) {
	switch tsc.servers[index].State {
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

// templateClusterServerIterator implement a server iterator,
// for a template cluster with multiple servers.
type templateClusterServerIterator struct {
	cluster *TemplateCluster
	cursor  int64
}

// Do implements ServerIterator.Do
func (it *templateClusterServerIterator) Do(action ardb.StorageAction) (reply interface{}, err error) {
	it.cluster.mux.RLock()
	cfg := it.cluster.servers[it.cursor]
	it.cluster.mux.RUnlock()

	return it.cluster.doAt(it.cursor, cfg, action)
}

// Next implements ServerIterator.Next
func (it *templateClusterServerIterator) Next() bool {
	it.cluster.mux.RLock()
	defer it.cluster.mux.RUnlock()

	if it.cursor >= it.cluster.serverCount {
		return false
	}

	var ok bool
	for it.cursor++; it.cursor < it.cluster.serverCount; it.cursor++ {
		if ok, _ = it.cluster.serverOperational(it.cursor); ok {
			return true
		}
	}

	return true
}

// storageClusterWatcher is a small helper struct,
// used to (un)set a storage cluster watcher for a given clusterID.
// By centralizing this logic,
// we only have to define it once and it keeps the callee's location clean.
type storageClusterWatcher struct {
	clusterID string
	channel   <-chan config.StorageClusterConfig
	cancel    context.CancelFunc
}

// Receive an update on the returned channel by the storageClusterWatcher.
func (scw *storageClusterWatcher) Receive() <-chan config.StorageClusterConfig {
	return scw.channel
}

// Close all open resources,
// openend and managed by this storageClusterWatcher
func (scw *storageClusterWatcher) Close() {
	if scw.cancel != nil {
		scw.cancel()
	}
}

// SetCluster allows you to (over)write the current internal cluster watcher.
// If the given clusterID is equal to the already used clusterID, nothing will happen.
// If the clusterID is different but the given one is nil, the current watcher will be stopped.
// In all other cases a new watcher will be attempted to be created,
// and used if succesfull (right before cancelling the old one), or otherwise an error is returned.
// In an error case the boolean parameter indicates whether a watcher is active or not.
func (scw *storageClusterWatcher) SetClusterID(ctx context.Context, cs config.Source, vdiskID, clusterID string) (bool, error) {
	if scw.clusterID == clusterID {
		// if the given ID is equal to the one we have stored internally,
		// we have nothing to do.
		// Returning true, such that no existing cluster info is deleted by accident.
		return scw.clusterID != "", nil
	}

	// if the given clusterID is nil, but ours isn't,
	// we'll simply want to close the watcher and clean up our internal state.
	if clusterID == "" {
		scw.cancel()
		scw.cancel = nil
		scw.clusterID = ""
		return false, nil // no watcher is active, as no cluster exists
	}

	// try to create the new watcher
	ctx, cancel := context.WithCancel(ctx)
	channel, err := config.WatchStorageClusterConfig(ctx, cs, clusterID)
	if err != nil {
		cs.MarkInvalidKey(config.Key{ID: vdiskID, Type: config.KeyVdiskNBD}, vdiskID)
		cancel()
		return false, err
	}

	// close the previous watcher
	scw.Close()

	// use the new watcher and set the new state
	scw.cancel = cancel
	scw.clusterID = clusterID
	scw.channel = channel
	return true, nil // a watcher is active, because the cluster exists
}

// Defined returns `true` if this storage cluster watcher
// has an internal watcher (for an existing cluster) defined.
func (scw *storageClusterWatcher) Defined() bool {
	return scw.clusterID != ""
}

// storageServersEqual compares if 2 storage server configs
// are equal, except for their state.
func storageServersEqual(a, b config.StorageServerConfig) bool {
	return a.Database == b.Database &&
		a.Address == b.Address
}

// mapErrorToBroadcastStatus maps the given error,
// returned by a `Connection` operation to a broadcast's message status.
func mapErrorToBroadcastStatus(err error) log.MessageStatus {
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return log.StatusServerTimeout
		}
		if netErr.Temporary() {
			return log.StatusServerTempError
		}
	} else if err == io.EOF {
		return log.StatusServerDisconnect
	}

	return log.StatusUnknownError
}

// enforces that our StorageClusters
// are actually StorageClusters
var (
	_ ardb.StorageCluster = (*PrimaryCluster)(nil)
	_ ardb.StorageCluster = (*TemplateCluster)(nil)
)

// enforces that our ServerIterators
// are actually ServerIterators
var (
	_ ardb.ServerIterator = (*primaryClusterServerIterator)(nil)
	_ ardb.ServerIterator = (*templateClusterServerIterator)(nil)
)

var (
	// ErrMethodNotSupported is an error returned
	// in case a method is called which is not supported by the object.
	ErrMethodNotSupported = errors.New("method is not supported")

	// ErrClusterNotDefined is an error returned
	// in case a cluster is used which is not defined.
	ErrClusterNotDefined = errors.New("ARDB storage cluster is not defined")
)
