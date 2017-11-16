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

// DoForAll implements StorageCluster.DoForAll
func (cluster *Cluster) DoForAll(pairs []ardb.IndexActionPair) ([]interface{}, error) {
	// [TODO] either remove `DoForAll` from the StorageCluster API,
	//        or support it in this implementation.
	panic("DoForAll is not supported for now")
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
// This cluster type does support hot-swapping of 2 online servers (which share the same index),
// meaning that prior to swapping, the data for this vdisk will be copied from the old to the new server.
// See `Cluster` for more information.
func NewPrimaryCluster(ctx context.Context, vdiskID string, cs config.Source) (*Cluster, error) {
	controller := &singleClusterStateController{
		vdiskID:       vdiskID,
		optional:      false,
		copyOnHotSwap: true,
		configSource:  cs,
		serverType:    log.ARDBPrimaryServer,
		getClusterID:  getPrimaryClusterID,
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
// This cluster type does support hot-swapping of 2 online servers (which share the same index),
// meaning that prior to swapping, the data for this vdisk will be copied from the old to the new server.
// See `Cluster` for more information.
func NewSlaveCluster(ctx context.Context, vdiskID string, optional bool, cs config.Source) (*Cluster, error) {
	controller := &singleClusterStateController{
		vdiskID:       vdiskID,
		optional:      optional,
		copyOnHotSwap: true,
		configSource:  cs,
		serverType:    log.ARDBSlaveServer,
		getClusterID:  getSlaveClusterID,
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
// This cluster type does support hot-swapping of 2 online servers (which share the same index),
// meaning that prior to swapping, the data for this vdisk will be copied from the old to the new server.
// See `Cluster` for more information.
func NewTemplateCluster(ctx context.Context, vdiskID string, optional bool, cs config.Source) (*Cluster, error) {
	controller := &singleClusterStateController{
		vdiskID:  vdiskID,
		optional: optional,
		// template cluster is read-only,
		// hot-swapping is entirely an external responsibility
		copyOnHotSwap: false,
		configSource:  cs,
		serverType:    log.ARDBTemplateServer,
		getClusterID:  getTemplateClusterID,
	}
	err := controller.spawnConfigReloader(ctx, cs)
	if err != nil {
		controller.Close()
		return nil, err
	}

	return NewCluster(vdiskID, controller)
}

type singleClusterStateController struct {
	vdiskID   string
	clusterID string

	// when true, it means it's acceptable for the cluster not to exist
	// otherwise this will be tracked as an error.
	optional bool

	// an optioan bool,
	// when enabled data will be copied from the old to the new server,
	// iff the new sever replaces the old server AND both are in state `online`
	copyOnHotSwap bool

	configSource config.Source

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
		log.Infof("couldn't update %s server for vdisk %s: index is OOB",
			ctrl.serverType, ctrl.vdiskID)
		return false // OOB
	}

	// ensure given config isn't out of date,
	// as we'll assume that when the given (dial) config differs from the used config,
	// the used config is correct and the given config is out of date.
	if !storageServersEqual(ctrl.servers[state.Index], state.Config) {
		log.Infof(
			"couldn't update %s server #%d for vdisk %s: "+
				"given config (%s) is out of date, it is now configured as %s",
			ctrl.serverType, state.Index, ctrl.vdiskID, &state.Config,
			&ctrl.servers[state.Index])
		return false // given config != used config
	}

	// update server state (if the state is new)
	return ctrl.setServerState(state.Index, state.Config.State)
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
	ctrl.clusterID = ctrl.getClusterID(vdiskNBDConfig)
	clusterExists, err := clusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, ctrl.clusterID)
	if err != nil {
		return err
	}
	if clusterExists {
		clusterCfg = <-clusterWatcher.Receive()
		ctrl.servers = clusterCfg.Servers
		ctrl.serverCount = int64(len(clusterCfg.Servers))
	} else if !ctrl.optional {
		return errors.Wrapf(ErrClusterNotDefined,
			"%s cluster %s does not exist", ctrl.serverType, ctrl.clusterID)
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

				ctrl.clusterID = ctrl.getClusterID(vdiskNBDConfig)
				clusterExists, err := clusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, ctrl.clusterID)
				if err != nil {
					log.Errorf("failed to watch new %s cluster %s: %v", ctrl.serverType, ctrl.clusterID, err)
					continue
				}
				if !clusterExists {
					if !ctrl.optional {
						log.Errorf("%s cluster no longer exists, while it is required for vdisk %s",
							ctrl.serverType, ctrl.vdiskID)
						// [TODO] Notify AYS about this error
					}
					ctrl.setServers(nil)
				}

			// handle cluster storage updates
			case clusterCfg = <-clusterWatcher.Receive():
				ctrl.setServers(clusterCfg.Servers)
			}
		}
	}()

	// all is operational, no error to return
	return nil
}

func (ctrl *singleClusterStateController) setServers(servers []config.StorageServerConfig) {
	ctrl.mux.Lock()
	defer ctrl.mux.Unlock()
	serverCount := int64(len(servers))

	// shrinking is now allowed, so handle this scenario
	if serverCount < ctrl.serverCount {
		// mark all "deleted" servers as state unknown, which will make this vdisk fail
		// [TODO] Notify AYS about this error
		for index := serverCount; index < ctrl.serverCount; index++ {
			ctrl.servers[index].State = config.StorageServerStateUnknown
		}
	} else if serverCount > ctrl.serverCount { // growing isn't allowed either
		// however if no serverCount was previously defined, we're OK with it
		if ctrl.serverCount == 0 { // simply set servers without needing to do any handling
			ctrl.servers = servers
			ctrl.serverCount = serverCount
			return
		}

		// ignore all extra servers
		// [TODO] Notify AYS about this error
		log.Errorf("hot-growing a cluster is not allowed, ignoring extra servers: %v", servers[ctrl.serverCount:])
		// trim input servers so we don't get an overflow error
		servers = servers[:ctrl.serverCount]
	}

	// update all servers
	var server, curServer config.StorageServerConfig
	for index := range servers {
		server, curServer = servers[index], ctrl.servers[index]

		// check if dial-config changes
		if !storageServersEqual(server, curServer) {
			if curServer.State == config.StorageServerStateOnline && curServer.State == server.State {
				// [TODO] Notify AYS about this error
				log.Errorf(
					"updating dial-config of online %s server #%d %s to %s, while hot-swapping is not supported (yet)",
					ctrl.serverType, index, &curServer, &server)
			} else {
				// [TODO] Notify AYS about server dial-config update
				log.Infof(
					"updating dial-config of %s server #%d (state: %s) %s "+
						"to %s (state: %s)",
					ctrl.serverType, index, &curServer, curServer.State, &server, server.State)
			}

			ctrl.servers[index].Address = server.Address
			ctrl.servers[index].Database = server.Database
		}

		// update state (if needed)
		ctrl.setServerState(int64(index), server.State)
	}
}

// setServerState allows you to update the state of a pre-configured server,
// the resulted boolean indicates whether or not an update took place.
func (ctrl *singleClusterStateController) setServerState(index int64, state config.StorageServerState) bool {
	old := ctrl.servers[index]
	if old.State == state {
		log.Debugf(
			"ignoring update for %s server #%d %s (state: %s), as it remains unchanged",
			ctrl.serverType, index, &old, old.State)
		return false
	}

	if old.State == config.StorageServerStateRIP && state != config.StorageServerStateRIP {
		// [TODO] Notify AYS about this error
		log.Errorf("cannot change state of %s server #%d %s as it is marked as RIP, forcing state to RIP as well (instead of %s)",
			ctrl.serverType, index, &old, state)
		state = config.StorageServerStateRIP
	}

	switch state {
	case config.StorageServerStateOnline:
		// [TODO] Notify AYS about this (unexpected?) event
		log.Infof(
			"marking %s server #%d %s (state: %s) (back?) as online",
			ctrl.serverType, index, &old, old.State)

	case config.StorageServerStateOffline:
		// [TODO] Notify AYS about this error
		log.Errorf(
			"marking %s server #%d %s (state: %s) as offline (no self-healing is possible)",
			ctrl.serverType, index, &old, old.State)

	case config.StorageServerStateRIP:
		// [TODO] Notify AYS about this error
		// [TODO] Support respreading of data (blocked by issue #601)
		log.Errorf(
			"marking %s server #%d %s (state: %s) as RIP (no respreading supported)",
			ctrl.serverType, index, &old, old.State)

	default:
		// [TODO] Notify AYS about this error
		log.Errorf(
			"marking %s server #%d %s (state: %s) forcefully as Unknown, as state %s is not supported",
			ctrl.serverType, index, &old, old.State, state)
		state = config.StorageServerStateUnknown
	}

	ctrl.servers[index].State = state
	return true
}

// getters to get a specific clusterID,
// used to  create the different kind of singleCluster controllers.
func getPrimaryClusterID(cfg config.VdiskNBDConfig) string  { return cfg.StorageClusterID }
func getSlaveClusterID(cfg config.VdiskNBDConfig) string    { return cfg.SlaveStorageClusterID }
func getTemplateClusterID(cfg config.VdiskNBDConfig) string { return cfg.TemplateStorageClusterID }

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
