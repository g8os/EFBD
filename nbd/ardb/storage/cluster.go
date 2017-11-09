package storage

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// [TODO]
// next steps:
// + add hot swapping of servers as a capability to the singleCluster;
// + find way to prevent/handle scenario where cluster grows/shrinks;
//
// check if we can hugely simplify the primarySlaveCluster controllers to only a few controllers:
// ... dunno if it is possible, but dunno, the current code is really huge
//   -> one pair controller (which has either 1 (primary/slave) or 2 servers active)
//   -> one undefinedStorageServer (undefined behaviour, returns a wrapped error) (is this one needed?!)
//   -> one deadServer
//   -> one nil server

// [TODO]
// => Figure out how to do the communication with the slave sync controller

// [TODO]
// => Make it possible that the NBDServer can keep using a slave server as a primary server,
//    even though there is no more primary server defined, and even if a primary server is offline

// NewCluster creates a new storage cluster for the given vdiskID,
// using the given controller to control the server state and fetch that state from.
func NewCluster(vdiskID string, controller ClusterStateController) (*Cluster, error) {
	if vdiskID == "" {
		return nil, errors.New("storage.Cluster requires a non-nil vdiskID")
	}
	if isInterfaceValueNil(controller) {
		return nil, errors.New("storage.Cluster requires a non-nil ClusterStateController")
	}
	return &Cluster{
		vdiskID:    vdiskID,
		pool:       ardb.NewPool(nil),
		controller: controller,
	}, nil
}

// Cluster defines a cluster which applies actions on servers,
// which are configured within a state controller.
// This state is both updated by external events (config hot reloading),
// as well as internal events (updating a server based on state changes).
type Cluster struct {
	vdiskID    string
	pool       *ardb.Pool
	controller ClusterStateController
}

// Do implements StorageCluster.Do
func (cluster *Cluster) Do(action ardb.StorageAction) (reply interface{}, err error) {
	var state ServerState
	// keep trying to apply action, until it works out,
	// or until no server is available any longer.
	for {
		state, err = cluster.controller.ServerState()
		if err != nil {
			// server wasn't avaialable for some illegal reason,
			// or no server was available at all
			return nil, err
		}

		// apply action, and return its results if the action was indeed applied.
		reply, err = cluster.applyAction(&state, action)
		if err != errActionNotApplied {
			return reply, err
		}
	}
}

// DoFor implements StorageCluster.DoFor
func (cluster *Cluster) DoFor(objectIndex int64, action ardb.StorageAction) (reply interface{}, err error) {
	var state ServerState
	// keep trying to apply action, until it works out,
	// or until no server is available any longer.
	for {
		state, err = cluster.controller.ServerStateFor(objectIndex)
		if err != nil {
			// server wasn't avaialable for some illegal reason,
			// or no server was available at all
			return nil, err
		}

		// apply action, and return its results if the action was indeed applied.
		reply, err = cluster.applyAction(&state, action)
		if err != errActionNotApplied {
			return reply, err
		}
	}
}

// ServerIterator implements StorageCluster.ServerIterator
func (cluster *Cluster) ServerIterator(ctx context.Context) (<-chan ardb.StorageServer, error) {
	ch := make(chan ardb.StorageServer)
	go func() {
		log.Debugf("starting server iterator for vdisk %s's storage.Cluster", cluster.vdiskID)
		defer func() {
			close(ch) // close channel iterator when finished
			log.Debugf("stopping server iterator for vdisk %s's storage.Cluster", cluster.vdiskID)
		}()

		for index := int64(0); index < cluster.controller.ServerCount(); index++ {
			server := smartServer{
				Index:   index,
				Cluster: cluster,
			}

			select {
			case ch <- server:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// ServerCount implements StorageCluster.ServerCount
func (cluster *Cluster) ServerCount() int64 {
	return cluster.controller.ServerCount()
}

// Close this storage cluster's open resources.
func (cluster *Cluster) Close() error {
	var slice errors.ErrorSlice
	slice.Add(cluster.controller.Close())
	slice.Add(cluster.pool.Close())
	return slice.AsError()
}

// applyAction applies the storage action to the server
// that can be dialer for the given action.
func (cluster *Cluster) applyAction(state *ServerState, action ardb.StorageAction) (reply interface{}, err error) {
	if state.Config.State != config.StorageServerStateOnline {
		return nil, ardb.ErrServerUnavailable
	}

	// try to open connection,
	// and apply the action to that connection if it could be dialed.
	conn, err := cluster.pool.Dial(state.Config)
	if err == nil {
		defer conn.Close()
		reply, err = action.Do(conn)
		if err == nil || errors.Cause(err) == ardb.ErrNil {
			return reply, err
		}
	}

	// mark the server as offline,
	// as something went wrong
	state.Config.State = config.StorageServerStateOffline
	if cluster.controller.UpdateServerState(*state) {
		// broadcast the error to AYS
		status := MapErrorToBroadcastStatus(err)
		log.Broadcast(
			status,
			log.SubjectStorage,
			log.ARDBServerTimeoutBody{
				Address:  state.Config.Address,
				Database: state.Config.Database,
				Type:     state.Type,
				VdiskID:  cluster.vdiskID,
			},
		)
	}

	return nil, errActionNotApplied
}

// smartServer defines an ardb.StorageServer returned
// by the default storage.Cluster, and applies a connection to
// whatever server that functions first for the given server index.
type smartServer struct {
	Index   int64
	Cluster *Cluster
}

// Do implements StorageServer.Do
func (server smartServer) Do(action ardb.StorageAction) (reply interface{}, err error) {
	var state ServerState
	// keep trying to apply action, until it works out,
	// or until no server is available any longer.
	for {
		state, err = server.Cluster.controller.ServerStateAt(server.Index)
		if err != nil {
			// server wasn't avaialable for some illegal reason,
			// or no server was available at all
			return nil, err
		}

		// apply action, and return its results if the action was indeed applied.
		reply, err = server.Cluster.applyAction(&state, action)
		if err != errActionNotApplied {
			return reply, err
		}
	}
}

// Config implements StorageServer.Config
func (server smartServer) Config() config.StorageServerConfig {
	state, err := server.Cluster.controller.ServerStateAt(server.Index)
	if err != nil {
		return config.StorageServerConfig{State: config.StorageServerStateRIP}
	}
	return state.Config
}

// ClusterStateController is used as the internal state controller for the storage Cluster.
// It is used to retrieve server configs and update server configs (internal and external).
type ClusterStateController interface {
	// Retrieve the first available server state.
	ServerState() (state ServerState, err error)
	// Retrieve the server state which maps to a given objectIndex.
	ServerStateFor(objectIndex int64) (state ServerState, err error)
	// Retrieve the server state at the given serverIndex.
	ServerStateAt(serverIndex int64) (state ServerState, err error)

	// Update the server state.
	// An update might be ignored if it is deemed to be out of date.
	// True is returned in case the update was applied.
	UpdateServerState(state ServerState) bool

	// ServerCount returns the (flat) amount of servers,
	// this state has in one dimension.
	ServerCount() int64

	// Close any open resources, previously in-use by this controller.
	Close() error
}

// ServerState is a snapshot of the state of a server,
// as it is retrieved from a ClusterStateController.
type ServerState struct {
	// Index of the server within the internal cluster (model)
	Index int64
	// Config of the server in its current state
	Config config.StorageServerConfig
	// Type of the server: {primary, slave, template}
	Type log.ARDBServerType
}

// NewPrimaryCluster creates a new PrimaryCluster.
// This cluster type supports config hot-reloading, but no self-healing of servers.
// See `Cluster` for more information.
func NewPrimaryCluster(ctx context.Context, vdiskID string, cs config.Source) (*Cluster, error) {
	controller := &singleClusterStateController{
		vdiskID:      vdiskID,
		optional:     false,
		serverType:   log.ARDBPrimaryServer,
		getClusterID: getPrimaryClusterID,
	}
	err := controller.spawnConfigReloader(ctx, cs)
	if err != nil {
		controller.Close()
		return nil, err
	}

	return NewCluster(vdiskID, controller)
}

// NewSlaveCluster creates a new SlaveCluster.
// This cluster type supports config hot-reloading, but no self-healing of servers.
// See `Cluster` for more information.
func NewSlaveCluster(ctx context.Context, vdiskID string, optional bool, cs config.Source) (*Cluster, error) {
	controller := &singleClusterStateController{
		vdiskID:      vdiskID,
		optional:     optional,
		serverType:   log.ARDBSlaveServer,
		getClusterID: getSlaveClusterID,
	}
	err := controller.spawnConfigReloader(ctx, cs)
	if err != nil {
		controller.Close()
		return nil, err
	}

	return NewCluster(vdiskID, controller)
}

// NewTemplateCluster creates a new TemplateCluster.
// This cluster type supports config hot-reloading, but no self-healing of servers.
// See `Cluster` for more information.
func NewTemplateCluster(ctx context.Context, vdiskID string, optional bool, cs config.Source) (*Cluster, error) {
	controller := &singleClusterStateController{
		vdiskID:      vdiskID,
		optional:     optional,
		serverType:   log.ARDBTemplateServer,
		getClusterID: getTemplateClusterID,
	}
	err := controller.spawnConfigReloader(ctx, cs)
	if err != nil {
		controller.Close()
		return nil, err
	}

	return NewCluster(vdiskID, controller)
}

type singleClusterStateController struct {
	vdiskID string

	// when true, it means it's acceptable for the cluster not to exist
	// otherwise this will be tracked as an error.
	optional bool

	serverType log.ARDBServerType

	servers     []config.StorageServerConfig
	serverCount int64

	mux sync.RWMutex

	cancel context.CancelFunc

	getClusterID func(cfg config.VdiskNBDConfig) string
}

// ServerState implements ClusterStateController.ServerState
func (ctrl *singleClusterStateController) ServerState() (state ServerState, err error) {
	ctrl.mux.RLock()
	defer ctrl.mux.RUnlock()

	if ctrl.serverCount == 0 {
		err = ErrClusterNotDefined
		return
	}

	state.Index, err = ardb.FindFirstServerIndex(ctrl.serverCount, ctrl.serverOperational)
	if err != nil {
		return
	}

	state.Config = ctrl.servers[state.Index]
	state.Type = ctrl.serverType
	return
}

// ServerStateFor implements ClusterStateController.ServerStateFor
func (ctrl *singleClusterStateController) ServerStateFor(objectIndex int64) (state ServerState, err error) {
	ctrl.mux.RLock()
	defer ctrl.mux.RUnlock()

	if ctrl.serverCount == 0 {
		err = ErrClusterNotDefined
		return
	}

	state.Index, err = ardb.ComputeServerIndex(ctrl.serverCount, objectIndex, ctrl.serverOperational)
	if err != nil {
		return
	}

	state.Config = ctrl.servers[state.Index]
	state.Type = ctrl.serverType
	return
}

// ServerStateAt implements ClusterStateController.ServerStateAt
func (ctrl *singleClusterStateController) ServerStateAt(serverIndex int64) (state ServerState, err error) {
	ctrl.mux.RLock()
	defer ctrl.mux.RUnlock()

	if ctrl.serverCount == 0 {
		err = ErrClusterNotDefined
		return
	}
	if serverIndex < 0 || serverIndex >= ctrl.serverCount {
		err = ardb.ErrServerIndexOOB
		return
	}

	state.Index = serverIndex
	state.Config = ctrl.servers[state.Index]
	state.Type = ctrl.serverType
	return
}

// UpdateServerState implements ClusterStateController.UpdateServerState
func (ctrl *singleClusterStateController) UpdateServerState(state ServerState) bool {
	ctrl.mux.Lock()
	defer ctrl.mux.Unlock()
	// ensure index is within range
	if state.Index >= ctrl.serverCount {
		log.Infof("couldn't update %s server for vdisk %s: index is OOB", ctrl.serverType, ctrl.vdiskID)
		return false // OOB
	}
	// ensure given config isn't out of date,
	// as we'll assume that when the given (dial) config differs from the used config,
	// the used config is correct and the given config is out of date.
	if !storageServersEqual(ctrl.servers[state.Index], state.Config) {
		log.Infof("couldn't update %s server for vdisk %s: given config (%s) is out of date, it is now configured as %s",
			ctrl.serverType, ctrl.vdiskID, state.Config.String(), ctrl.servers[state.Index].String())
		return false // given config != used config
	}
	// ensure the given config's state isn't already equal to the used config's state.
	if state.Config.State == ctrl.servers[state.Index].State {
		log.Infof("couldn't update %s server (%s) for vdisk %s: state (%s) remains unchanged",
			ctrl.serverType, ctrl.servers[state.Index].String(), ctrl.vdiskID, ctrl.servers[state.Index].State)
		return false // no update happened
	}

	// update applied
	ctrl.servers[state.Index].State = state.Config.State
	return true
}

// ServerCount implements ClusterStateController.ServerCount
func (ctrl *singleClusterStateController) ServerCount() int64 {
	ctrl.mux.RLock()
	count := ctrl.serverCount
	ctrl.mux.RUnlock()
	return count
}

// Close implements ClusterStateController.Close
func (ctrl *singleClusterStateController) Close() error {
	ctrl.cancel()
	return nil
}

// serverOperational returns if a server is operational
func (ctrl *singleClusterStateController) serverOperational(index int64) (bool, error) {
	switch ctrl.servers[index].State {
	case config.StorageServerStateOnline:
		return true, nil
	case config.StorageServerStateRIP:
		return false, nil
	default:
		return false, ardb.ErrServerUnavailable
	}
}

// spawnConfigReloader starts all needed config watchers,
// and spawns a goroutine to receive the updates.
// An error is returned in case the initial watch-creation and config-update failed.
// All future errors will be logged without stopping this goroutine.
func (ctrl *singleClusterStateController) spawnConfigReloader(ctx context.Context, cs config.Source) error {
	// create the context and cancelFunc used for the master watcher.
	ctx, ctrl.cancel = context.WithCancel(ctx)

	// create the master watcher if possible
	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, ctrl.vdiskID)
	if err != nil {
		return err
	}
	vdiskNBDConfig := <-vdiskNBDRefCh

	var clusterCfg config.StorageClusterConfig

	// create the storage cluster watcher,
	// and execute the initial config update iff
	// an internal watcher is created.
	var clusterWatcher ClusterConfigWatcher
	clusterID := ctrl.getClusterID(vdiskNBDConfig)
	clusterExists, err := clusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, clusterID)
	if err != nil {
		return err
	}
	if clusterExists {
		clusterCfg = <-clusterWatcher.Receive()
		ctrl.servers = clusterCfg.Servers
		ctrl.serverCount = int64(len(clusterCfg.Servers))
	} else if !ctrl.optional {
		return errors.Wrapf(ErrClusterNotDefined,
			"%s cluster %s does not exist", ctrl.serverType, clusterID)
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

				clusterID = ctrl.getClusterID(vdiskNBDConfig)
				clusterExists, err := clusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, clusterID)
				if err != nil {
					log.Errorf("failed to watch new %s cluster %s: %v", ctrl.serverType, clusterID, err)
					continue
				}
				if !clusterExists {
					if !ctrl.optional {
						log.Errorf("%s cluster no longer exists, while it is required for vdisk %s",
							ctrl.serverType, ctrl.vdiskID)
						// [TODO] Notify AYS about this
					}
					ctrl.mux.Lock()
					ctrl.servers = nil
					ctrl.serverCount = 0
					ctrl.mux.Unlock()
				}

			// handle cluster storage updates
			case clusterCfg = <-clusterWatcher.Receive():
				ctrl.mux.Lock()
				ctrl.servers = clusterCfg.Servers
				ctrl.serverCount = int64(len(clusterCfg.Servers))
				ctrl.mux.Unlock()
			}
		}
	}()

	// all is operational, no error to return
	return nil
}

// getters to get a specific clusterID,
// used to  create the different kind of singleCluster controllers.
func getPrimaryClusterID(cfg config.VdiskNBDConfig) string  { return cfg.StorageClusterID }
func getSlaveClusterID(cfg config.VdiskNBDConfig) string    { return cfg.SlaveStorageClusterID }
func getTemplateClusterID(cfg config.VdiskNBDConfig) string { return cfg.TemplateStorageClusterID }

// SlaveSyncController defines the interface of a controller,
// which allows us to start/stop the syncing of one or multiple slave servers,
// such that we can start writing to it without getting ourself in race conditions
// with the regular slave syncer.
type SlaveSyncController interface {
	// StartSlaveSync commands the controller to
	// start syncing one or multiple slave servers for a given vdisk.
	// An error should be returned in case the syncing could not happen.
	StartSlaveSync(vdiskID string, indices ...int64) error
	// StopSlaveSync commands the controller to
	// stop syncing one or multiple slave servers for a given vdisk.
	// An error should be returned in case the syncing couldn't be stopped.
	StopSlaveSync(vdiskID string, indices ...int64) error
}

type primarySlaveClusterPairStateController struct {
	vdiskID                          string
	primaryClusterID, slaveClusterID string
	slaveSyncer                      SlaveSyncController
	configSource                     config.Source

	servers     []primarySlavePairController
	serverCount int64

	mux sync.RWMutex

	cancel context.CancelFunc
}

// ServerStateFor implements ClusterStateController.ServerStateFor
func (ctrl *primarySlaveClusterPairStateController) ServerState() (ServerState, error) {
	ctrl.mux.RLock()
	defer ctrl.mux.RUnlock()

	if ctrl.serverCount == 0 {
		return ServerState{}, ErrClusterNotDefined
	}

	index, err := ardb.FindFirstServerIndex(ctrl.serverCount, ctrl.serverOperational)
	if err != nil {
		return ServerState{}, err
	}
	return ctrl.servers[index].ServerState(index)
}

// ServerStateFor implements ClusterStateController.ServerStateFor
func (ctrl *primarySlaveClusterPairStateController) ServerStateFor(objectIndex int64) (ServerState, error) {
	ctrl.mux.RLock()
	defer ctrl.mux.RUnlock()

	if ctrl.serverCount == 0 {
		return ServerState{}, ErrClusterNotDefined
	}

	index, err := ardb.ComputeServerIndex(ctrl.serverCount, objectIndex, ctrl.serverOperational)
	if err != nil {
		return ServerState{}, err
	}
	return ctrl.servers[index].ServerState(index)
}

// ServerStateAt implements ClusterStateController.ServerStateAt
func (ctrl *primarySlaveClusterPairStateController) ServerStateAt(serverIndex int64) (ServerState, error) {
	ctrl.mux.RLock()
	defer ctrl.mux.RUnlock()

	if ctrl.serverCount == 0 {
		return ServerState{}, ErrClusterNotDefined
	}
	if serverIndex < 0 || serverIndex >= ctrl.serverCount {
		return ServerState{}, ardb.ErrServerIndexOOB
	}
	return ctrl.servers[serverIndex].ServerState(serverIndex)
}

// UpdateServerState implements ClusterStateController.UpdateServerState
func (ctrl *primarySlaveClusterPairStateController) UpdateServerState(state ServerState) bool {
	ctrl.mux.Lock()
	defer ctrl.mux.Unlock()

	// ensure index is within range
	if state.Index >= ctrl.serverCount {
		log.Infof("couldn't update %s server for vdisk %s: index is OOB", state.Type, ctrl.vdiskID)
		return false // OOB
	}

	ctx := &primarySlavePairContext{
		vdiskID:          ctrl.vdiskID,
		primaryClusterID: ctrl.primaryClusterID,
		slaveClusterID:   ctrl.slaveClusterID,
		configSource:     ctrl.configSource,
		slaveSyncer: &serverSlaveSyncer{
			vdiskID:     ctrl.vdiskID,
			index:       state.Index,
			slaveSyncer: ctrl.slaveSyncer,
		},
	}

	var err error
	// update the state of the server, using the defined primary/slave server config
	switch state.Type {
	case log.ARDBPrimaryServer:
		ctrl.servers[state.Index], err = ctrl.servers[state.Index].SetPrimaryServerState(ctx, state.Config)
	case log.ARDBSlaveServer:
		ctrl.servers[state.Index], err = ctrl.servers[state.Index].SetSlaveServerState(ctx, state.Config)
	default:
		panic("unsupported server type update in NBD PrimarySlaveClusterPair")
	}

	if err != nil {
		log.Errorf("couldn't update %s server #%d for vdisk %s: %v",
			state.Type, state.Index, ctrl.vdiskID, err)
		return false
	}

	return true
}

func (ctrl *primarySlaveClusterPairStateController) setPrimaryClusterConfig(cfg config.StorageClusterConfig) error {
	panic("TODO")
}

func (ctrl *primarySlaveClusterPairStateController) setSlaveClusterConfig(cfg config.StorageClusterConfig) error {
	panic("TODO")
}

// ServerCount implements ClusterStateController.ServerCount
func (ctrl *primarySlaveClusterPairStateController) ServerCount() int64 {
	ctrl.mux.RLock()
	count := ctrl.serverCount
	ctrl.mux.RUnlock()
	return count
}

// Close implements ClusterStateController.Close
func (ctrl *primarySlaveClusterPairStateController) Close() error {
	ctrl.cancel()
	return nil
}

func (ctrl *primarySlaveClusterPairStateController) serverOperational(index int64) (bool, error) {
	return ctrl.servers[index].IsOperational()
}

// spawnConfigReloader starts all needed config watchers,
// and spawns a goroutine to receive the updates.
// An error is returned in case the initial watch-creation and config-update failed.
// All future errors will be logged without stopping this goroutine.
func (ctrl *primarySlaveClusterPairStateController) spawnConfigReloader(ctx context.Context, cs config.Source) error {
	// create the context and cancelFunc used for the master watcher.
	ctx, ctrl.cancel = context.WithCancel(ctx)

	// create the master watcher if possible
	vdiskNBDRefCh, err := config.WatchVdiskNBDConfig(ctx, cs, ctrl.vdiskID)
	if err != nil {
		return err
	}
	vdiskNBDConfig := <-vdiskNBDRefCh

	var clusterCfg config.StorageClusterConfig
	var primClusterWatcher, slaveClusterWatcher ClusterConfigWatcher

	// create the primary storage cluster watcher,
	// and execute the initial config update iff
	// an internal watcher is created.
	clusterExists, err := primClusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.StorageClusterID)
	if err != nil {
		return err
	}
	if !clusterExists {
		panic("primary cluster should always exist on a non-error path")
	}
	// update the clusterID in the context
	ctrl.primaryClusterID = vdiskNBDConfig.StorageClusterID
	// update all configured primary servers
	err = ctrl.setPrimaryClusterConfig(<-primClusterWatcher.Receive())
	if err != nil {
		return err
	}

	// do the same for the slave cluster watcher
	clusterExists, err = slaveClusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.SlaveStorageClusterID)
	if err != nil {
		return err
	}

	// update the clusterID in the context
	ctrl.slaveClusterID = vdiskNBDConfig.SlaveStorageClusterID
	if clusterExists {
		// update all configured slave servers
		err = ctrl.setSlaveClusterConfig(<-slaveClusterWatcher.Receive())
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

				// update primary cluster config watcher
				clusterExists, err = primClusterWatcher.SetClusterID(
					ctx, cs, ctrl.vdiskID, vdiskNBDConfig.StorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new primary cluster %s: %v",
						vdiskNBDConfig.StorageClusterID, err)
				} else if !clusterExists {
					panic("primary cluster should always exist on a non-error path")
				}
				ctrl.primaryClusterID = vdiskNBDConfig.StorageClusterID

				// update slave cluster config watcher
				clusterExists, err = slaveClusterWatcher.SetClusterID(
					ctx, cs, ctrl.vdiskID, vdiskNBDConfig.SlaveStorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new slave cluster %s: %v",
						vdiskNBDConfig.SlaveStorageClusterID, err)
				}
				ctrl.slaveClusterID = vdiskNBDConfig.SlaveStorageClusterID

				// unset slave cluster if it no longer exists
				if !clusterExists {
					log.Infof("vdisk %s no longer has a slave cluster defined", ctrl.vdiskID)
					err = ctrl.setSlaveClusterConfig(config.StorageClusterConfig{})
					if err != nil {
						log.Errorf("couldn't undefine slave cluster config for vdisk %s: %v",
							ctrl.vdiskID, err)
					}
				}

			// handle primary cluster storage updates
			case clusterCfg = <-primClusterWatcher.Receive():
				err = ctrl.setPrimaryClusterConfig(clusterCfg)
				if err != nil {
					log.Errorf("couldn't set primary cluster config %s for vdisk %s: %v",
						vdiskNBDConfig.StorageClusterID, ctrl.vdiskID, err)
					cs.MarkInvalidKey(config.Key{
						ID:   vdiskNBDConfig.StorageClusterID,
						Type: config.KeyClusterStorage,
					}, ctrl.vdiskID)
				}

			// handle slave cluster storage updates
			case clusterCfg = <-slaveClusterWatcher.Receive():
				err = ctrl.setSlaveClusterConfig(clusterCfg)
				if err != nil {
					log.Errorf("couldn't set slave cluster config %s for vdisk %s: %v",
						vdiskNBDConfig.SlaveStorageClusterID, ctrl.vdiskID, err)
					cs.MarkInvalidKey(config.Key{
						ID:   vdiskNBDConfig.SlaveStorageClusterID,
						Type: config.KeyClusterStorage,
					}, ctrl.vdiskID)
				}
			}
		}
	}()

	// all is operational, no error to return
	return nil
}

type primarySlavePairController interface {
	ServerState(index int64) (ServerState, error)
	IsOperational() (bool, error)

	// Overwrite or delete the primary server config.
	SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error)
	// Overwrite or delete the slave server config.
	SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error)

	// Update the state (and only the state) of the primary config only
	// if possible AND if the dial information equals the given dial config.
	SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error)
	// Update the state (and only the state) of the slave config only
	// if possible AND if the dial information equals the given dial config.
	SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error)
}

// a context shared with the primarySlavePairController,
// while updating or transforming primarySlavePairController
// by overwriting or deleting server configs.
type primarySlavePairContext struct {
	vdiskID                          string
	primaryClusterID, slaveClusterID string
	configSource                     config.Source
	slaveSyncer                      slaveSyncServerController
}

type slaveSyncServerController interface {
	// StartSlaveSync commands the slave sync controller to
	// start syncing a given slave server for a given vdisk.
	// An error will be returned in case the syncing could not happen.
	StartSlaveSync() error

	// StopSlaveSync commands the slave sync controller to
	// stop syncing a given slave server for a given vdisk.
	// An error will be returned in case the syncing couldn't be stopped.
	StopSlaveSync() error
}

type serverSlaveSyncer struct {
	vdiskID     string
	index       int64
	slaveSyncer SlaveSyncController
}

// StartSlaveSync commands the internal slave sync controller to
// start syncing a given slave server for a given vdisk.
// An error will be returned in case the syncing could not happen.
func (ctrl *serverSlaveSyncer) StartSlaveSync() error {
	return ctrl.slaveSyncer.StartSlaveSync(ctrl.vdiskID, ctrl.index)
}

// StopSlaveSync commands the internal slave sync controller to
// stop syncing a given slave server for a given vdisk.
// An error will be returned in case the syncing couldn't be stopped.
func (ctrl *serverSlaveSyncer) StopSlaveSync() error {
	return ctrl.slaveSyncer.StopSlaveSync(ctrl.vdiskID, ctrl.index)
}

type undefinedServer struct{}

func (s undefinedServer) ServerState(index int64) (ServerState, error) {
	return ServerState{}, ErrServerNotDefined
}
func (s undefinedServer) IsOperational() (bool, error) { return false, ErrServerNotDefined }
func (s undefinedServer) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		return s, errors.Wrap(errNopConfigUpdate, "no primary server given, and no primary/slave server was defined")
	}
	return &primaryServer{cfg: *cfg}, nil
}
func (s undefinedServer) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		return s, errors.Wrap(errNopConfigUpdate, "no slave server given, and no primary/slave server was defined")
	}
	return &slaveServer{cfg: *cfg}, nil
}
func (s undefinedServer) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	return s, errors.Wrap(errInvalidConfigUpdate, "can't set state of non-existing primary server")
}
func (s undefinedServer) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	return s, errors.Wrap(errInvalidConfigUpdate, "can't set state of non-existing slave server")
}

type deadServer struct{}

func (s deadServer) ServerState(index int64) (ServerState, error) {
	return ServerState{}, ErrServerIsDead
}
func (s deadServer) IsOperational() (bool, error) { return false, nil } // no error though
func (s deadServer) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		return s, errors.Wrap(errNopConfigUpdate, "no primary server given, and primary/slave servers are dead")
	}
	return s, errors.Wrapf(errInvalidConfigUpdate, "ignoring update of primary server %s (state: %s), as server (pair) is marked as RIP", cfg)
}
func (s deadServer) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		return s, errors.Wrap(errNopConfigUpdate, "no slave server given, and primary/slave servers are dead")
	}
	return s, errors.Wrapf(errInvalidConfigUpdate, "ignoring update of slave server %s (state: %s), as server (pair) is marked as RIP", cfg)
}
func (s deadServer) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	return s, errors.Wrapf(errInvalidConfigUpdate, "ignoring update of primary server %s (state: %s), as server (pair) is marked as RIP", cfg)
}
func (s deadServer) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	return s, errors.Wrapf(errInvalidConfigUpdate, "ignoring update of slave server %s (state: %s), as server (pair) is marked as RIP", cfg)
}

type primaryServer struct {
	cfg config.StorageServerConfig
}

func (s *primaryServer) ServerState(index int64) (ServerState, error) {
	if s.cfg.State != config.StorageServerStateOnline {
		if s.cfg.State == config.StorageServerStateRIP {
			panic("this server type should /never/ be in RIP mode ")
		}
		return ServerState{}, ardb.ErrServerUnavailable
	}

	return ServerState{
		Index:  index,
		Config: s.cfg,
		Type:   log.ARDBPrimaryServer,
	}, nil
}
func (s *primaryServer) IsOperational() (bool, error) {
	switch s.cfg.State {
	case config.StorageServerStateOnline:
		return true, nil
	case config.StorageServerStateRIP:
		panic("this server type should /never/ be in RIP mode ")
	default:
		return false, ardb.ErrServerUnavailable
	}
}
func (s *primaryServer) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		log.Errorf("deleting primary server (%s) without a slave server to back it up", &s.cfg)
		return undefinedServer{}, nil
	}

	// either ignore or swap the servers if both are online
	if s.cfg.State == config.StorageServerStateOnline && cfg.State == config.StorageServerStateOnline {
		if storageServersEqual(s.cfg, *cfg) {
			return s, errors.Wrapf(errNopConfigUpdate,
				"unpaired primary server %s (state: %s) remains unchanged", cfg, cfg.State)
		}

		log.Infof("swapping online (unpaired and used) primary server %s with %s", &s.cfg, cfg)
		err := repairStorageServer(ctx.vdiskID, ctx.configSource, s.cfg, *cfg)
		if err != nil {
			// [TODO] Notify AYS about this error
			log.Errorf("couldn't swap online (unpaired and used) primary server: %v", err)
			log.Errorf("marking primary server %s as state unknown, making it unavailable", cfg)

			// update
			s.cfg = *cfg
			s.cfg.State = config.StorageServerStateUnknown
			return s, nil
		}

		// update config
		s.cfg = *cfg
		return s, nil
	}

	if cfg.State == config.StorageServerStateRIP {
		log.Errorf("marking primary server %s as RIP, making this server (pair) permanently unavailable", &s.cfg)
		return deadServer{}, nil
	}

	log.Infof("updating primary server from %s to %s (NOTE: that no self-healing will and can be done)", &s.cfg, cfg)
	s.cfg = *cfg
	return s, nil
}
func (s *primaryServer) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		return s, errors.Wrapf(errNopConfigUpdate,
			"can't delete non-existing slave server in unpaired primary server %s", &s.cfg)
	}

	switch s.cfg.State {
	case config.StorageServerStateOnline:
		// simply upgrade to primarySlaveCluster,
		// as we don't care what the slave server config is at this point
		return &primarySlaveServerPair{
			primary: s.cfg,
			slave:   *cfg,
		}, nil

	case config.StorageServerStateOffline:
		if s.cfg.State == config.StorageServerStateOnline {
			// First ensure that we can safely use slave server as primary server,
			// without getting ourselves into a race condition with the regular slave syncer
			err := ctx.slaveSyncer.StopSlaveSync()
			if err != nil {
				// [TODO] notify AYS about this error case
				// regular slave syncer couldn't be stopped
				return &unavailablePrimarySlaveServerPair{
					primary: s.cfg,
					slave:   *cfg,
					reason:  errors.Wrap(err, "slave syncer couldn't be stopped"),
				}, nil
			}

			// slave syncer was stopped, and now we can use slave server as the primary server
			return &slavePrimaryServerPair{
				primary: s.cfg,
				slave:   *cfg,
			}, nil
		}

		return &unavailablePrimarySlaveServerPair{
			primary: s.cfg,
			slave:   *cfg,
			reason:  errors.New("primary server is offline and no slave server is available"),
		}, nil

	case config.StorageServerStateRIP:
		log.Errorf("marking unexisting slave server as RIP, "+
			" making this server (pair) (incl. paired primary  server %s) permanently unavailable", &s.cfg)
		return deadServer{}, nil
	}

	return &unavailablePrimarySlaveServerPair{
		primary: s.cfg,
		slave:   *cfg,
		reason:  errors.Newf("can't switch to slave server as primary server is in unexpected state %s", s.cfg.State),
	}, nil
}
func (s *primaryServer) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(s.cfg, cfg) {
		return s, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of primary server (%s) to state %s, as given config (%s) defines different server",
			&s.cfg, cfg.State, &cfg)
	}
	if s.cfg.State == cfg.State {
		return s, errors.Wrapf(errNopConfigUpdate, "primary server %s has already the desired state %s", &s.cfg, s.cfg.State)
	}

	if cfg.State == config.StorageServerStateRIP {
		log.Errorf("marking primary server %s as RIP, making this server (pair) permanently unavailable", &s.cfg)
		return deadServer{}, nil
	}

	log.Infof("updating primary server (%s) state from %s to %s", &s.cfg, &s.cfg.State, cfg.State)
	s.cfg.State = cfg.State
	return s, nil
}
func (s *primaryServer) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	return s, errors.Wrapf(errInvalidConfigUpdate,
		"can't set state of non-existing slave server, primary server %s remains unchanged", &s.cfg)
}

type slaveServer struct {
	cfg config.StorageServerConfig
}

func (s *slaveServer) ServerState(index int64) (ServerState, error) {
	return ServerState{}, ardb.ErrServerUnavailable
}
func (s *slaveServer) IsOperational() (bool, error) { return false, ardb.ErrServerUnavailable }
func (s *slaveServer) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		return s, errors.Wrapf(errNopConfigUpdate,
			"can't delete non-existing primary server, slave server %s remains unchanged", &s.cfg)
	}
	if cfg.State != config.StorageServerStateOnline {
		if cfg.State == config.StorageServerStateRIP {
			log.Errorf("marking unexisting primary server as RIP, making this server (pair) "+
				"(incl. previous defined slave server %s) permanently unavailable", &s.cfg)
			return deadServer{}, nil
		}
		log.Infof(
			"pairing unavailable primary server %s with unavailable slave server %s, keeing the pair unavailable",
			cfg, &s.cfg)
		return &unavailablePrimarySlaveServerPair{
			primary: *cfg,
			slave:   s.cfg,
			reason:  errors.New("neither the primary or slave server is online"),
		}, nil
	}

	// not copying from slave to primary,
	// as the config was restored from some invalid state,
	// and thus we'll asume that the external user takes full responsibility
	log.Infof("promoting unavailable slave server (%s) to a primarySlaveServerPair, "+
		"using the newly configured primary server (%s)", &s.cfg, cfg)
	return &primarySlaveServerPair{primary: *cfg, slave: s.cfg}, nil
}
func (s *slaveServer) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		log.Infof("deleting disabled slave server (%s), making the server pair undefined", s.cfg.String())
		return undefinedServer{}, nil
	}

	if s.cfg.Equal(*cfg) {
		return s, errors.Wrapf(errNopConfigUpdate,
			"slave server %s (state: %s) remains unchanged", cfg, cfg.State)
	}
	if cfg.State == config.StorageServerStateRIP {
		log.Errorf("marking slave server %s (previous state: %s) as RIP, making this server (pair) "+
			"permanently unavailable", &s.cfg, s.cfg.State)
		return deadServer{}, nil
	}

	log.Infof("updating disabled slave server from %s to %s", s.cfg.String(), cfg.String())
	s.cfg = *cfg
	return s, nil
}
func (s *slaveServer) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	return s, errors.Wrap(errInvalidConfigUpdate, "can't set state of non-existing primary server")
}
func (s *slaveServer) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(s.cfg, cfg) {
		return s, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of slave server (%s) to state %s, as given config (%s) defines different server",
			&s.cfg, cfg.State, &cfg)
	}
	if s.cfg.State == cfg.State {
		return s, errors.Wrapf(errNopConfigUpdate, "slave server %s has already the desired state %s", &s.cfg, s.cfg.State)
	}
	if cfg.State == config.StorageServerStateRIP {
		log.Errorf("marking slave server %s (previous state: %s) as RIP, making this server (pair) "+
			"permanently unavailable", &s.cfg, s.cfg.State)
		return deadServer{}, nil
	}

	log.Infof("updating slave server (%s) state from %s to %s", &s.cfg, &s.cfg.State, cfg.State)
	s.cfg.State = cfg.State
	return s, nil
}

type primarySlaveServerPair struct {
	primary, slave config.StorageServerConfig
}

func (p *primarySlaveServerPair) ServerState(index int64) (ServerState, error) {
	if p.primary.State != config.StorageServerStateOnline {
		panic("primarySlaveServerPair requires an online primary server")
	}

	return ServerState{
		Index:  index,
		Config: p.primary,
		Type:   log.ARDBPrimaryServer,
	}, nil
}
func (p *primarySlaveServerPair) IsOperational() (bool, error) {
	// this type is only used when primary server is configured, used AND online.
	// Thus we do not need to check anything
	return true, nil
}
func (p *primarySlaveServerPair) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		// [TODO] warn AYS about this error
		log.Errorf("disabling primary server (%s), making this server index unavailable", &p.primary)
		return &slaveServer{p.slave}, nil
	}

	switch cfg.State {
	case config.StorageServerStateOnline:
		if storageServersEqual(p.primary, *cfg) {
			return p, errors.Wrapf(errNopConfigUpdate,
				"primary server %s (state: %s) remains unchanged", cfg, cfg.State)
		}

		log.Infof("swapping online (paired and used) primary server %s with %s", &p.primary, cfg)
		err := repairStorageServer(ctx.vdiskID, ctx.configSource, p.primary, *cfg)
		if err != nil {
			// [TODO] Notify AYS about this error
			log.Errorf("couldn't swap online (paired and used) primary server: %v", err)
			return &unavailablePrimarySlaveServerPair{
				primary: *cfg,
				slave:   p.slave,
				reason:  errors.Wrapf(err, "couldn't swap online (paired and used) primary server %s with %s", &p.primary, cfg),
			}, nil
		}

		// update config
		p.primary = *cfg
		return p, nil

	case config.StorageServerStateOffline:
		// as long as there is a slave to back it up,
		// we can try to switch to it
		if p.slave.State == config.StorageServerStateOnline {
			log.Infof("bringing primary server (%s) offline, "+
				"attempting to use slave server (%s) as the primary instead", cfg, &p.slave)
			// communicate to regular slave syncer that it should stop syncing to this slave server
			err := ctx.slaveSyncer.StopSlaveSync()
			if err != nil {
				// [TODO] notify AYS about this error case
				// regular slave syncer couldn't be stopped
				return &unavailablePrimarySlaveServerPair{
					primary: *cfg,
					slave:   p.slave,
					reason:  errors.Wrap(err, "slave syncer couldn't be stopped"),
				}, nil
			}

			// start using slave server as primary server (but keep also the new primary cfg in memory)
			return &slavePrimaryServerPair{
				primary: *cfg,
				slave:   p.slave,
			}, nil
		}
	}

	// in all other scenarios the pair becomes unavailable
	log.Errorf("primary server (%s) becomes unavailable due to unexpected state %s, "+
		"making this pair unavailable", cfg, cfg.State)
	return &unavailablePrimarySlaveServerPair{
		primary: *cfg,
		slave:   p.slave,
		reason:  errors.Newf("unexpected primary server state change to %s", cfg.State),
	}, nil
}
func (p *primarySlaveServerPair) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		log.Errorf("deleting slave server (%s), leaving the primary server (%s) without a backup server",
			&p.slave, &p.primary)
		return &primaryServer{p.primary}, nil
	}

	if p.slave.Equal(*cfg) {
		return p, errors.Wrapf(errNopConfigUpdate,
			"slave server %s (state: %s) remains unchanged", cfg, cfg.State)
	}

	// some state checking, purely for logging purposes
	if cfg.State == config.StorageServerStateOnline && p.slave.State != config.StorageServerStateOnline {
		log.Infof("enabling slave server (%s), to be used as backup for the primary server (%s)",
			cfg, &p.primary)
	} else if cfg.State != config.StorageServerStateOnline && p.slave.State == config.StorageServerStateOnline {
		log.Errorf("disabling slave server (%s), leaving the primary server (%s) without backup",
			cfg, &p.primary)
	}

	// update slave config
	log.Infof("swapping (unused) slave server %s with %s", &p.slave, cfg)
	p.slave = *cfg
	return p, nil
}
func (p *primarySlaveServerPair) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(p.primary, cfg) {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired primary server (%s) to state %s, as given config (%s) defines different server",
			&p.primary, cfg.State, &cfg)
	}

	switch cfg.State {
	case config.StorageServerStateOnline:
		return p, errors.Wrapf(errNopConfigUpdate,
			"paired primary server %s (state: %s) remains unchanged", cfg, cfg.State)

	case config.StorageServerStateOffline:
		// as long as there is a slave to back it up,
		// we can try to switch to it
		if p.slave.State == config.StorageServerStateOnline {
			log.Infof("bringing primary server (%s) offline, "+
				"attempting to use slave server (%s) as the primary instead", cfg, &p.slave)
			// communicate to regular slave syncer that it should stop syncing to this slave server
			err := ctx.slaveSyncer.StopSlaveSync()
			if err != nil {
				// [TODO] notify AYS about this error case
				// regular slave syncer couldn't be stopped
				return &unavailablePrimarySlaveServerPair{
					primary: cfg,
					slave:   p.slave,
					reason:  errors.Wrap(err, "slave syncer couldn't be stopped"),
				}, nil
			}

			// start using slave server as primary server (but keep also the new primary cfg in memory)
			return &slavePrimaryServerPair{
				primary: cfg,
				slave:   p.slave,
			}, nil
		}
	}

	// in all other scenarios the pair becomes unavailable
	log.Errorf("paired primary server (%s) becomes unavailable due to unexpected state %s, "+
		"making this pair unavailable", cfg, cfg.State)
	return &unavailablePrimarySlaveServerPair{
		primary: cfg,
		slave:   p.slave,
		reason:  errors.Newf("unexpected paired primary server state change to %s", cfg.State),
	}, nil
}
func (p *primarySlaveServerPair) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(p.slave, cfg) {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired (unused) slave server (%s) to state %s, as given config (%s) defines different server",
			&p.slave, cfg.State, &cfg)
	}

	if p.slave.State == cfg.State {
		return p, errors.Wrapf(errNopConfigUpdate,
			"paired (unused) slave server %s (state: %s) remains unchanged", &cfg, cfg.State)
	}

	// some state checking, purely for logging purposes
	if cfg.State == config.StorageServerStateOnline && p.slave.State != config.StorageServerStateOnline {
		log.Infof("enabling paired (unused) slave server (%s), to be used as backup for the primary server (%s)",
			&cfg, &p.primary)
	} else if cfg.State != config.StorageServerStateOnline && p.slave.State == config.StorageServerStateOnline {
		log.Errorf("disabling paired (unused) slave server (%s), leaving the primary server (%s) without backup",
			&cfg, &p.primary)
	}

	// update slave server state
	log.Infof("updating paired (unused) slave server (%s) state from %s to %s", &p.slave, &p.slave.State, cfg.State)
	p.slave.State = cfg.State
	return p, nil
}

type unavailablePrimarySlaveServerPair struct {
	primary, slave config.StorageServerConfig
	reason         error
}

func (p *unavailablePrimarySlaveServerPair) ServerState(index int64) (ServerState, error) {
	return ServerState{}, errors.WrapError(ardb.ErrServerUnavailable, p.reason)
}
func (p *unavailablePrimarySlaveServerPair) IsOperational() (bool, error) {
	return false, errors.WrapError(ardb.ErrServerUnavailable, p.reason)
}
func (p *unavailablePrimarySlaveServerPair) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		log.Errorf("deleting unavailable primary server %s, keeping this serverPair unavailable", &p.primary)
		return &slaveServer{cfg: p.slave}, nil
	}
	if p.primary.Equal(*cfg) {
		return p, errors.Wrapf(errNopConfigUpdate,
			"primary server %s (state: %s) remains unchanged", cfg, cfg.State)
	}

	// ensure that if a primary server was marked as RIP,
	// it doesn't change state once again
	if p.primary.State == config.StorageServerStateRIP {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update paired (unavailable) primary server (%s) to %s (state: %s), as it is marked as RIP",
			&p.primary, cfg, cfg.State, &cfg)
	}

	if cfg.State == config.StorageServerStateOnline {
		log.Infof("making unavailable serverPair available by switching to primary server %s", cfg)
		return &primarySlaveServerPair{
			primary: *cfg,
			slave:   p.slave,
		}, nil
	}

	// simply update the already broken primary cfg
	log.Infof("swapping (unavailable and paired) primary server %s with %s", &p.primary, cfg)
	p.primary = *cfg
	return p, nil
}
func (p *unavailablePrimarySlaveServerPair) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		log.Errorf("deleting unavailable slave server %s, doing nothing to fix the unavailable serverPair", &p.slave)
		return &primaryServer{cfg: p.primary}, nil
	}

	if p.primary.State == config.StorageServerStateOffline && cfg.State == config.StorageServerStateOnline {
		log.Infof("attempting to make unavailable serverPair available by switching to slave server %s", cfg)
		// as long as there is a slave to back it up,
		// we can try to switch to it
		// communicate to regular slave syncer that it should stop syncing to this slave server
		err := ctx.slaveSyncer.StopSlaveSync()
		if err != nil {
			// [TODO] notify AYS about this error case
			// regular slave syncer couldn't be stopped
			// simply update slave and error reason
			p.slave = *cfg
			p.reason = errors.Wrap(err, "slave syncer couldn't be stopped")
			return p, nil
		}

		// start using slave server as primary server (but keep also the new primary cfg in memory)
		return &slavePrimaryServerPair{
			primary: p.primary,
			slave:   *cfg,
		}, nil
	}

	if p.slave.Equal(*cfg) {
		return p, errors.Wrapf(errNopConfigUpdate,
			"slave server %s (state: %s) remains unchanged", cfg, cfg.State)
	}

	log.Infof("updating unavailable slave server %s, doing nothing to fix the unavailable serverPair", cfg)
	p.slave = *cfg
	return p, nil
}
func (p *unavailablePrimarySlaveServerPair) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(p.primary, cfg) {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired (unavailable) primary server (%s) to state %s, as given config (%s) defines different server",
			&p.primary, cfg.State, &cfg)
	}
	if p.primary.State == cfg.State {
		return p, errors.Wrapf(errNopConfigUpdate,
			"paired (unused) primary server %s (state: %s) remains unchanged", &cfg, cfg.State)
	}

	// ensure that if a primary server was marked as RIP,
	// it doesn't change state once again
	if p.primary.State == config.StorageServerStateRIP {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired (unavailable) primary server (%s) to state %s, as it is marked as RIP",
			&p.primary, cfg.State, &cfg)
	}

	if cfg.State == config.StorageServerStateOnline {
		log.Infof("making unavailable serverPair available by switching to primary server %s", cfg)
		return &primarySlaveServerPair{
			primary: cfg,
			slave:   p.slave,
		}, nil
	}

	log.Infof("changing state of (unavailable and paired) primary server (%s) from %s to %s",
		&p.primary, p.primary.State, cfg.State)
	p.primary.State = cfg.State
	return p, nil
}
func (p *unavailablePrimarySlaveServerPair) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(p.slave, cfg) {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired (unavailable) slave server (%s) to state %s, as given config (%s) defines different server",
			&p.slave, cfg.State, &cfg)
	}

	if cfg.State == config.StorageServerStateOnline {
		log.Infof("attempting to make unavailable serverPair available by switching to slave server %s", cfg)
		// as long as there is a slave to back it up,
		// we can try to switch to it
		// communicate to regular slave syncer that it should stop syncing to this slave server
		err := ctx.slaveSyncer.StopSlaveSync()
		if err != nil {
			// [TODO] notify AYS about this error case
			// regular slave syncer couldn't be stopped
			// simply update slave and error reason
			p.slave = cfg
			p.reason = errors.Wrap(err, "slave syncer couldn't be stopped")
			return p, nil
		}

		// start using slave server as primary server (but keep also the new primary cfg in memory)
		return &slavePrimaryServerPair{
			primary: p.primary,
			slave:   cfg,
		}, nil
	}

	if p.slave.State == cfg.State {
		return p, errors.Wrapf(errNopConfigUpdate,
			"paired (unused) slave server %s (state: %s) remains unchanged", &cfg, cfg.State)
	}

	log.Infof("changing state of (unavailable and paired) slave server (%s) from %s to %s",
		&p.slave, p.slave.State, cfg.State)
	p.slave.State = cfg.State
	return p, nil
}

type slavePrimaryServerPair struct {
	slave, primary config.StorageServerConfig
}

func (p *slavePrimaryServerPair) ServerState(index int64) (ServerState, error) {
	if p.slave.State != config.StorageServerStateOnline {
		panic("slavePrimaryServerPair requires an online slave server")
	}
	return ServerState{
		Index:  index,
		Config: p.slave,
		Type:   log.ARDBSlaveServer,
	}, nil
}
func (p *slavePrimaryServerPair) IsOperational() (bool, error) {
	// this type is only used when slave server is used AND online,
	// and thus we do not need to check anything
	return true, nil
}
func (p *slavePrimaryServerPair) SetPrimaryServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		// switch to pure slave server (still using that as the primary server)
		log.Infof("deleting (unused and previously paired) primary server (%s), "+
			"demoting slave server %s to become a pure slave server", &p.primary, cfg)
		return &slaveServer{cfg: p.slave}, nil
	}
	if p.primary.Equal(*cfg) {
		return p, errors.Wrapf(errNopConfigUpdate,
			"primary server %s (state: %s) remains unchanged", cfg, cfg.State)
	}
	return p.handleValidPrimaryServerConfigUpdate(ctx, *cfg)
}
func (p *slavePrimaryServerPair) SetSlaveServerConfig(ctx *primarySlavePairContext, cfg *config.StorageServerConfig) (primarySlavePairController, error) {
	if cfg == nil {
		log.Errorf(
			"deleting used (and previously paired) slave server %s, "+
				"making this pair unavailable by switching to unavailable primary server %s", &p.slave, &p.primary)
		return &primaryServer{cfg: p.primary}, nil
	}

	if cfg.State == config.StorageServerStateOnline {
		if storageServersEqual(p.slave, *cfg) {
			return p, errors.Wrapf(errNopConfigUpdate,
				"paired and used slave server %s (state: %s) remains unchanged", cfg, cfg.State)
		}

		log.Infof("swapping online (paired and used) slave server %s with %s", &p.slave, cfg)
		err := repairStorageServer(ctx.vdiskID, ctx.configSource, p.slave, *cfg)
		if err != nil {
			// [TODO] Notify AYS about this error
			log.Errorf("couldn't swap online (paired and used) slave server: %v", err)
			return &unavailablePrimarySlaveServerPair{
				primary: p.primary,
				slave:   *cfg,
				reason:  errors.Wrapf(err, "couldn't swap online (paired and used) slave server %s with %s", &p.slave, cfg),
			}, nil
		}

		p.slave = *cfg // update config
		return p, nil
	}

	log.Errorf(
		"making (paired and previously used) slave server %s unavailable, making this pair unavailable", cfg)
	// [TODO] Warn AYS
	return &unavailablePrimarySlaveServerPair{
		primary: p.primary,
		slave:   *cfg,
		reason:  errors.Newf("slave server became unavailable (state: %s), while using it as a primary server", cfg.State),
	}, nil // switch to broken pair
}
func (p *slavePrimaryServerPair) SetPrimaryServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(p.primary, cfg) {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired (unavailable) primary server (%s) to state %s, as given config (%s) defines different server",
			&p.primary, cfg.State, &cfg)
	}
	if p.primary.State == cfg.State {
		return p, errors.Wrapf(errNopConfigUpdate,
			"paired (unused) primary server %s (state: %s) remains unchanged", &cfg, cfg.State)
	}
	return p.handleValidPrimaryServerConfigUpdate(ctx, cfg)
}
func (p *slavePrimaryServerPair) SetSlaveServerState(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	if !storageServersEqual(p.slave, cfg) {
		return p, errors.Wrapf(errInvalidConfigUpdate,
			"can't update state of paired (and used) slave server (%s) to state %s, as given config (%s) defines different server",
			&p.slave, cfg.State, &cfg)
	}

	if cfg.State == config.StorageServerStateOnline {
		return p, errors.Wrapf(errNopConfigUpdate,
			"paired and used slave server %s (state: %s) remains unchanged", &p.slave, cfg.State)
	}

	log.Errorf(
		"making (paired and previously used) slave server %s unavailable, making this pair unavailable", cfg)
	// [TODO] Warn AYS
	return &unavailablePrimarySlaveServerPair{
		primary: p.primary,
		slave:   cfg,
		reason:  errors.Newf("slave server became unavailable (state: %s), while using it as a primary server", cfg.State),
	}, nil // switch to broken pair
}
func (p *slavePrimaryServerPair) handleValidPrimaryServerConfigUpdate(ctx *primarySlavePairContext, cfg config.StorageServerConfig) (primarySlavePairController, error) {
	switch cfg.State {
	case config.StorageServerStateOnline, config.StorageServerStateRepair:
		log.Infof("attempting to use %s as the primary server "+
			" and paired with slave server %s (which was previously used as primary server)", &cfg, &p.slave)
		log.Infof("copying data from slave server %s to (new) primary server %s", &p.slave, &cfg)
		err := repairStorageServer(ctx.vdiskID, ctx.configSource, p.slave, cfg)
		if err != nil {
			// [TODO] Notify AYS about this error
			log.Errorf("couldn't swap online (paired and used) primary server: %v", err)
			return &unavailablePrimarySlaveServerPair{
				primary: cfg,
				slave:   p.slave,
				reason: errors.Wrapf(err,
					"couldn't use online (newly paired) primary server %s (instead of slave server %s)", &cfg, &p.slave),
			}, nil
		}

		// contact slave syncer to warn it can sync to slave server once again,
		// we don't really care if it cannot start the syncing though,
		// we'll continue either way
		if err := ctx.slaveSyncer.StartSlaveSync(); err != nil {
			// [TODO] Notify AYS about this error case
			log.Errorf("slave syncer couldn't start syncing to server %s once again", p.slave)
		} else {
			log.Infof("slave syncer started syncing to server %s once again", &p.slave)
		}

		// repairing was succesful,
		// return fixed primarySlave pair and start using primary again
		return &primarySlaveServerPair{
			primary: config.StorageServerConfig{
				Address:  cfg.Address,
				Database: cfg.Database,
				State:    config.StorageServerStateOnline,
			},
			slave: p.slave,
		}, nil

	case config.StorageServerStateRespread:
		// TODO.... Respread...

		// respreading was succesful,
		// now mark this pair as unavaialble, by marking the primary server as RIP
		log.Infof("making ServerPair unavailable, stop using slave server %s, "+
			"because primary primary server (%s, state: %s), "+
			"was killed after data from paired slave server was respreaded over the primary cluster",
			&p.slave, &p.primary, p.primary.State)
		return &unavailablePrimarySlaveServerPair{
			primary: config.StorageServerConfig{
				Address:  cfg.Address,
				Database: cfg.Database,
				State:    config.StorageServerStateRIP,
			},
			slave: p.slave,
		}, nil

	case config.StorageServerStateRIP:
		log.Infof("making ServerPair unavailable, stop using slave server %s, "+
			"because primary primary server (%s, state: %s), was updated to %s (state: %s)",
			&p.slave, &p.primary, p.primary.State, &cfg, cfg.State)
		return &unavailablePrimarySlaveServerPair{
			primary: cfg,
			slave:   p.slave,
		}, nil
	}

	// simply update the primary server config
	log.Infof(
		"swapping (unused) primary server %s (state: %s) with %s (state: %s), pairing it with the used slave server %s",
		&p.primary, p.primary.State, &cfg, cfg.State, &p.slave)
	p.primary = cfg
	return p, nil // and return the same slave-used pair
}

// ClusterConfigWatcher is a small helper struct,
// used to (un)set a storage cluster watcher for a given clusterID.
// By centralizing this logic,
// we only have to define it once and it keeps the callee's location clean.
type ClusterConfigWatcher struct {
	clusterID string
	channel   <-chan config.StorageClusterConfig
	cancel    context.CancelFunc
}

// Receive an update on the returned channel by the ClusterConfigWatcher.
func (ccw *ClusterConfigWatcher) Receive() <-chan config.StorageClusterConfig {
	return ccw.channel
}

// Close all open resources,
// openend and managed by this ClusterWatcher
func (ccw *ClusterConfigWatcher) Close() {
	if ccw.cancel != nil {
		ccw.cancel()
	}
}

// SetClusterID allows you to (over)write the current internal cluster watcher.
// If the given clusterID is equal to the already used clusterID, nothing will happen.
// If the clusterID is different but the given one is nil, the current watcher will be stopped.
// In all other cases a new watcher will be attempted to be created,
// and used if succesfull (right before cancelling the old one), or otherwise an error is returned.
// In an error case the boolean parameter indicates whether a watcher is active or not.
func (ccw *ClusterConfigWatcher) SetClusterID(ctx context.Context, cs config.Source, vdiskID, clusterID string) (bool, error) {
	if ccw.clusterID == clusterID {
		// if the given ID is equal to the one we have stored internally,
		// we have nothing to do.
		// Returning true, such that no existing cluster info is deleted by accident.
		return ccw.clusterID != "", nil
	}

	// if the given clusterID is nil, but ours isn't,
	// we'll simply want to close the watcher and clean up our internal state.
	if clusterID == "" {
		ccw.cancel()
		ccw.cancel = nil
		ccw.clusterID = ""
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
	ccw.Close()

	// use the new watcher and set the new state
	ccw.cancel = cancel
	ccw.clusterID = clusterID
	ccw.channel = channel
	return true, nil // a watcher is active, because the cluster exists
}

// Defined returns `true` if this storage cluster watcher
// has an internal watcher (for an existing cluster) defined.
func (ccw *ClusterConfigWatcher) Defined() bool {
	return ccw.clusterID != ""
}

// copies data for a given vdisk from a source- to a target- storage server.
func repairStorageServer(vdiskID string, cs config.Source, source, target config.StorageServerConfig) error {
	// get static vdisk info
	staticCfg, err := config.ReadVdiskStaticConfig(cs, vdiskID)
	if err != nil {
		return errors.Wrapf(err, "couldn't repair storage server %s", &target)
	}
	cfg := CopyVdiskConfig{
		VdiskID:   vdiskID,
		Type:      staticCfg.Type,
		BlockSize: int64(staticCfg.BlockSize),
	}

	// create uni clusters
	sourceCluster, err := ardb.NewUniCluster(source, nil)
	if err != nil {
		return errors.Wrapf(err, "couldn't repair storage server %s", &target)
	}
	targetCluster, err := ardb.NewUniCluster(target, nil)
	if err != nil {
		return errors.Wrapf(err, "couldn't repair storage server %s", &target)
	}

	// Copy the data from the source to the target cluster (see: repair a storage server)
	err = CopyVdisk(cfg, cfg, sourceCluster, targetCluster)
	if err != nil {
		return errors.Wrapf(err, "couldn't repair storage server %s", &target)
	}
	return nil
}

// copy and respread data from a slave server to a primary cluster for a given vdisk
func respreadStorageServer(vdiskID string, cs config.Source, source config.StorageServerConfig, target config.StorageClusterConfig) error {
	// get static vdisk info
	staticCfg, err := config.ReadVdiskStaticConfig(cs, vdiskID)
	if err != nil {
		return errors.Wrapf(err, "couldn't respread storage server %s", &target)
	}
	cfg := CopyVdiskConfig{
		VdiskID:   vdiskID,
		Type:      staticCfg.Type,
		BlockSize: int64(staticCfg.BlockSize),
	}

	// create source uni cluster
	sourceCluster, err := ardb.NewUniCluster(source, nil)
	if err != nil {
		return errors.Wrapf(err, "couldn't respread storage server %s", &target)
	}

	// create target cluster
	pool := ardb.NewPool(nil)
	defer pool.Close()
	targetCluster, err := ardb.NewCluster(target, pool)
	if err != nil {
		return errors.Wrapf(err, "couldn't respread storage server %s", &target)
	}

	// Copy and respreadthe data from the source server to the target cluster
	err = CopyVdisk(cfg, cfg, sourceCluster, targetCluster)
	if err != nil {
		return errors.Wrapf(err, "couldn't respread storage server %s", &target)
	}
	return nil
}

// storageServersEqual compares if 2 storage server configs
// are equal, except for their state.
func storageServersEqual(a, b config.StorageServerConfig) bool {
	return a.Database == b.Database &&
		a.Address == b.Address
}

// MapErrorToBroadcastStatus maps the given error,
// returned by a `Connection` operation to a broadcast's message status.
func MapErrorToBroadcastStatus(err error) log.MessageStatus {
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return log.StatusServerTimeout
		}
		if netErr.Temporary() {
			return log.StatusServerTempError
		}
	} else if errors.Cause(err) == io.EOF {
		return log.StatusServerDisconnect
	}

	return log.StatusUnknownError
}

// enforces that our StorageClusters
// are actually StorageClusters
var (
	_ ardb.StorageCluster = (*Cluster)(nil)
)

// enforces that our ServerIterators
// are actually ServerIterators
var (
	_ ardb.StorageServer = smartServer{}
)

// enforces that our ClusterStateControllers
// are actually ClusterStateControllers
var (
	_ ClusterStateController = (*primarySlaveClusterPairStateController)(nil)
	_ ClusterStateController = (*singleClusterStateController)(nil)
)

var (
	// ErrMethodNotSupported is an error returned
	// in case a method is called which is not supported by the object.
	ErrMethodNotSupported = errors.New("method is not supported")

	// ErrClusterNotDefined is an error returned
	// in case a cluster is used which is not defined.
	ErrClusterNotDefined = errors.New("ARDB storage cluster is not defined")

	// ErrServerNotDefined is returned when no server in a pair is defined
	ErrServerNotDefined = errors.New("ARDB server is not defined")

	// ErrServerIsDead is returned when an ARDB server (pair) is dead
	ErrServerIsDead = errors.New("ARDB server (pair) is  dead")
)

// internal erros which should never propagate to the user
var (
	errActionNotApplied    = errors.New("storage action not applied")
	errNopConfigUpdate     = errors.New("no config update was required")
	errInvalidConfigUpdate = errors.New("invalid config update")
)
