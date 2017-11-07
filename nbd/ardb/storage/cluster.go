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
// => Figure out how to do the communication with the slave sync controller

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

// NewTemplateCluster creates a new TemplateCluster.
// See `Cluster` for more information.
func NewTemplateCluster(ctx context.Context, vdiskID string, cs config.Source) (*Cluster, error) {
	controller := &templateClusterStateController{vdiskID: vdiskID}
	err := controller.spawnConfigReloader(ctx, cs)
	if err != nil {
		controller.Close()
		return nil, err
	}

	return NewCluster(vdiskID, controller)
}

type templateClusterStateController struct {
	vdiskID string

	servers     []config.StorageServerConfig
	serverCount int64

	mux sync.RWMutex

	cancel context.CancelFunc
}

// ServerState implements ClusterStateController.ServerState
func (ctrl *templateClusterStateController) ServerState() (state ServerState, err error) {
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
	state.Type = log.ARDBTemplateServer
	return
}

// ServerStateFor implements ClusterStateController.ServerStateFor
func (ctrl *templateClusterStateController) ServerStateFor(objectIndex int64) (state ServerState, err error) {
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
	state.Type = log.ARDBTemplateServer
	return
}

// ServerStateAt implements ClusterStateController.ServerStateAt
func (ctrl *templateClusterStateController) ServerStateAt(serverIndex int64) (state ServerState, err error) {
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
	state.Type = log.ARDBTemplateServer
	return
}

// UpdateServerState implements ClusterStateController.UpdateServerState
func (ctrl *templateClusterStateController) UpdateServerState(state ServerState) bool {
	ctrl.mux.Lock()
	defer ctrl.mux.Unlock()
	// ensure index is within range
	if state.Index >= ctrl.serverCount {
		log.Debugf("couldn't update template server for vdisk %s: index is OOB", ctrl.vdiskID)
		return false // OOB
	}
	if state.Config.Equal(ctrl.servers[state.Index]) {
		log.Debugf("couldn't update template server for vdisk %s: state remains unchanged", ctrl.vdiskID)
		return false // no update happened
	}

	// update applied
	ctrl.servers[state.Index] = state.Config
	return true
}

// ServerCount implements ClusterStateController.ServerCount
func (ctrl *templateClusterStateController) ServerCount() int64 {
	ctrl.mux.RLock()
	count := ctrl.serverCount
	ctrl.mux.RUnlock()
	return count
}

// Close implements ClusterStateController.Close
func (ctrl *templateClusterStateController) Close() error {
	ctrl.cancel()
	return nil
}

// serverOperational returns if a server is operational
func (ctrl *templateClusterStateController) serverOperational(index int64) (bool, error) {
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
func (ctrl *templateClusterStateController) spawnConfigReloader(ctx context.Context, cs config.Source) error {
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
	clusterExists, err := clusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.TemplateStorageClusterID)
	if err != nil {
		return err
	}
	if clusterExists {
		clusterCfg = <-clusterWatcher.Receive()
		ctrl.servers = clusterCfg.Servers
		ctrl.serverCount = int64(len(clusterCfg.Servers))
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

				clusterExists, err := clusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.TemplateStorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new template cluster %s: %v",
						vdiskNBDConfig.TemplateStorageClusterID, err)
					continue
				}
				if !clusterExists {
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

type primarySlaveClusterPairStateController struct {
	vdiskID string

	servers                       []primarySlavePairController
	serverCount, slaveServerCount int64

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

	// update the state of the server, using the defined primary/slave server config
	switch state.Type {
	case log.ARDBPrimaryServer:
		ctrl.servers[state.Index] = ctrl.servers[state.Index].SetPrimaryServerConfig(&state.Config)
	case log.ARDBSlaveServer:
		ctrl.servers[state.Index] = ctrl.servers[state.Index].SetSlaveServerConfig(&state.Config)
	default:
		panic("unsupported server type update in NBD PrimarySlaveClusterPair")
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
	err = ctrl.setPrimaryClusterConfig(<-primClusterWatcher.Receive())
	if err != nil {
		return err
	}

	// do the same for the slave cluster watcher
	clusterExists, err = slaveClusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.SlaveStorageClusterID)
	if err != nil {
		return err
	}
	if clusterExists {
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
				clusterExists, err = primClusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.StorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new primary cluster %s: %v",
						vdiskNBDConfig.StorageClusterID, err)
				} else if !clusterExists {
					panic("primary cluster should always exist on a non-error path")
				}

				// update slave cluster config watcher
				clusterExists, err = slaveClusterWatcher.SetClusterID(ctx, cs, ctrl.vdiskID, vdiskNBDConfig.SlaveStorageClusterID)
				if err != nil {
					log.Errorf("failed to watch new slave cluster %s: %v",
						vdiskNBDConfig.SlaveStorageClusterID, err)
				}
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

// [TODO]
// => figure out how we can access stuff like the slave-sync-controller
//    from within the SetPrimaryServerConfig/SetSlaveServerConfig calls

/*
what about...

type SlaveSyncController interface {
	StartSlaveSync(indices ...int64) error
	StopSlaveSync(indices ...int64) error
}

// this could be used within the SetPrimaryServerConfig/SetSlaveServerConfig calls
type primarySlavePairContext struct {
	SlaveSyncer SlaveSyncController
}

*/

type primarySlavePairController interface {
	ServerState(index int64) (ServerState, error)
	IsOperational() (bool, error)

	SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController
	SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController
}

type undefinedServer struct{}

func (s undefinedServer) ServerState(index int64) (ServerState, error) {
	return ServerState{}, ErrServerNotDefined
}
func (s undefinedServer) IsOperational() (bool, error) { return false, ErrServerNotDefined }
func (s undefinedServer) SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		return s
	}
	return &primaryServer{cfg: *cfg}
}
func (s undefinedServer) SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		return s
	}
	return &slaveServer{cfg: *cfg}
}

type primaryServer struct {
	cfg config.StorageServerConfig
}

func (s *primaryServer) ServerState(index int64) (ServerState, error) {
	if s.cfg.State != config.StorageServerStateOnline {
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
		return false, nil
	default:
		return false, ardb.ErrServerUnavailable
	}
}
func (s *primaryServer) SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		log.Errorf("deleting primary server (%s) without a slave server to back it up", &s.cfg)
		return undefinedServer{}
	}

	log.Infof("updating primary server from %s to %s", &s.cfg, cfg)
	s.cfg = *cfg
	return s
}
func (s *primaryServer) SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		return s // nothing to do
	}

	if s.cfg.State == config.StorageServerStateOnline {
		// simply upgrade to primarySlaveCluster,
		// as we don't care what the slave server config is at this point
		return &primarySlaveServerPair{
			primary: s.cfg,
			slave:   *cfg,
		}
	}

	if s.cfg.State == config.StorageServerStateOffline {
		if s.cfg.State == config.StorageServerStateOnline {
			// [TODO]: First ensure that we can safely use slave server as primary server (e.g. talk to tlogserver first)
			return &slavePrimaryServerPair{
				primary: s.cfg,
				slave:   *cfg,
			}
		}

		return &unavailablePrimarySlaveServerPair{
			primary: s.cfg,
			slave:   *cfg,
			reason:  errors.New("primary server is offline and no slave server is available"),
		}
	}

	return &unavailablePrimarySlaveServerPair{
		primary: s.cfg,
		slave:   *cfg,
		reason:  errors.Newf("can't switch to slave server as primary server is in unexpected state %s", s.cfg.State),
	}
}

type slaveServer struct {
	cfg config.StorageServerConfig
}

func (s *slaveServer) ServerState(index int64) (ServerState, error) {
	return ServerState{}, ardb.ErrServerUnavailable
}
func (s *slaveServer) IsOperational() (bool, error) { return false, ardb.ErrServerUnavailable }
func (s *slaveServer) SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		return s // nothing to do
	}
	if cfg.State != config.StorageServerStateOnline {
		log.Infof(
			"pairing unavaialble primary server %s with unavailable slave server %s, keeing the pair unavailable",
			cfg, &s.cfg)
		return &unavailablePrimarySlaveServerPair{
			primary: *cfg,
			slave:   s.cfg,
			reason:  errors.New("neither the primary or slave server is online"),
		}
	}

	// not copying from slave to primary,
	// as the config was restored from some invalid state,
	// and thus we'll asume that the external user takes full responsibility
	log.Infof("promoting unavailable slave server (%s) to a primarySlaveServerPair, "+
		"using the newly configured primary server (%s)", &s.cfg, cfg)
	return &primarySlaveServerPair{primary: *cfg, slave: s.cfg}
}
func (s *slaveServer) SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		log.Infof("deleting disabled slave server (%s), making the server pair undefined", s.cfg.String())
		return undefinedServer{}
	}

	log.Infof("updating disabled slave server from %s to %s", s.cfg.String(), cfg.String())
	s.cfg = *cfg
	return s
}

type primarySlaveServerPair struct {
	primary, slave config.StorageServerConfig
}

func (p *primarySlaveServerPair) ServerState(index int64) (ServerState, error) {
	if p.primary.State != config.StorageServerStateOnline {
		return ServerState{}, ardb.ErrServerUnavailable
	}

	return ServerState{
		Index:  index,
		Config: p.primary,
		Type:   log.ARDBPrimaryServer,
	}, nil
}
func (p *primarySlaveServerPair) IsOperational() (bool, error) {
	switch p.primary.State {
	case config.StorageServerStateOnline:
		return true, nil
	case config.StorageServerStateRIP:
		return false, nil
	default:
		return false, ardb.ErrServerUnavailable
	}
}
func (p *primarySlaveServerPair) SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		// [TODO] warn AYS about this error
		log.Errorf("disabling primary server (%s), making this server index unavailable", &p.primary)
		return &slaveServer{p.slave}
	}

	switch cfg.State {
	case config.StorageServerStateOnline:
		if !p.primary.Equal(*cfg) {
			log.Infof("swapping online (paired and used) primary server  %s with %s", &p.primary, cfg)
			// [TODO] Copy: PS -> PS'
			/*
				if err != nil {
					// [TODO] log error
					// [TODO] notify AYS
					return &unavailablePrimarySlaveServerPair{
						primary: *cfg,
						slave:   p.slave,
						reason: err,
					}
				}
			*/

			// update config
			p.primary = *cfg
		}
		return p

	case config.StorageServerStateOffline:
		// as long as there is a slave to back it up,
		// we can try to switch to it
		if p.slave.State == config.StorageServerStateOnline {
			log.Infof("bringing primary server (%s) offline, "+
				"attempting to use slave server (%s) as the primary instead", cfg, &p.slave)
			// switch to slave server
			// [TODO]: communicate to tlogserver that it should stop syncing to slave server

			if true { // [TODO] replace by `err == nil`
				// start using slave server (but keep also the new primary cfg in memory)
				return &slavePrimaryServerPair{
					primary: *cfg,
					slave:   p.slave,
				}
			}
			/* else {
				// [TODO] log error AND notify AYS
			} */
		}
	}

	// in all other scenarios the pair becomes unavailable
	log.Errorf("primary server (%s) becomes unavailable due to unexpected state %s, "+
		"making this pair unavailable", cfg, cfg.State)
	return &unavailablePrimarySlaveServerPair{
		primary: *cfg,
		slave:   p.slave,
		reason:  errors.Newf("unexpected primary server state change to %s", cfg.State),
	}
}
func (p *primarySlaveServerPair) SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		log.Errorf("deleting slave server (%s), leaving the primary server (%s) without a backup server",
			&p.slave, &p.primary)
		return &primaryServer{p.primary}
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
	return p
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
func (p *unavailablePrimarySlaveServerPair) SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		log.Errorf("deleting unavailable primary server %s, keeping this serverPair unavailable", &p.primary)
		return &slaveServer{cfg: p.slave}
	}

	if cfg.State == config.StorageServerStateOnline {
		log.Infof("making unavailable serverPair available by switching to primary server %s", cfg)
		return &primarySlaveServerPair{
			primary: *cfg,
			slave:   p.slave,
		}
	}

	// simply update the already broken primary cfg
	log.Errorf("swapping (unavailable and paired) primary server %s with %s", &p.primary, cfg)
	p.primary = *cfg
	return p
}
func (p *unavailablePrimarySlaveServerPair) SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		log.Errorf("deleting unavailable slave server %s, doing nothing to fix the unavailable serverPair", &p.slave)
		return &primaryServer{cfg: p.primary}
	}

	if p.primary.State == config.StorageServerStateOffline && cfg.State == config.StorageServerStateOnline {
		log.Infof("attempting to make unavailable serverPair available by switching to slave server %s", cfg)
		// [TODO] communicate with tlogserver to stop syncing to slave server
		return &slavePrimaryServerPair{
			primary: p.primary,
			slave:   *cfg,
		}
	}

	log.Errorf("updating unavailable slave server %s, doing nothing to fix the unavailable serverPair", cfg)
	p.slave = *cfg
	return p
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
func (p *slavePrimaryServerPair) SetPrimaryServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		// switch to pure slave server (which is just an unavailable dummy really)
		// [TODO] communicate with Tlogserver that it can start syncing to slave server again
		log.Infof("deleting (unused and previously paired) primary server (%s), "+
			"demoting slave server %s to become unused and unavailable", &p.primary, cfg)
		return &slaveServer{cfg: p.slave}
	}

	if cfg.State == config.StorageServerStateOnline {
		log.Infof("attempting to make unavailable serverPair available by using %s as the primary server "+
			" to be used and paired with slave server %s", cfg, &p.slave)
		// [TODO] copy SS -> PS
		// ... on error  what to return AND DO warn AYS
		// [TODO] contact tlogserver to warn it can sync to slave server once again
		// ... What to do with error?
		// Warn AYS on error too?

		// repairing was succesful,
		// return fixed primarySlave pair and start using primary again
		return &primarySlaveServerPair{
			primary: *cfg,
			slave:   p.slave,
		}
	}

	// simply update the primary server config
	log.Infof(
		"swapping (unused) primary server %s with %s, pairing it with the used slave server %s",
		&p.primary, cfg, &p.slave)
	p.primary = *cfg
	return p // and return the same slave-used pair
}
func (p *slavePrimaryServerPair) SetSlaveServerConfig(cfg *config.StorageServerConfig) primarySlavePairController {
	if cfg == nil {
		log.Errorf(
			"deleting used (and previously paired) slave server %s, "+
				"making this pair unavaialble by switching to unavaialble primary server %s", &p.slave, &p.primary)
		return &primaryServer{cfg: p.primary}
	}

	if cfg.State == config.StorageServerStateOnline {
		log.Infof("swapping used (and paired) slave server %s with %s", &p.slave, cfg)
		// [TODO] Copy: SS -> SS'
		// ... on error -> switch to unavaialble cluster
		p.slave = *cfg // update config
		return p
	}

	log.Errorf(
		"making (paired and previously used) slave server %s unavaialble, making this pair unavaialble", cfg)
	// [TODO] Warn AYS
	return &unavailablePrimarySlaveServerPair{
		primary: p.primary,
		slave:   *cfg,
		reason:  errors.Newf("slave server became unavaialble (state: %s), while using it as a primary server", cfg.State),
	} // switch to broken pair
}

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

// [TODO]
// => delete all this deprecated primary cluster code
//    and replace it with a `NewPrimarySlaveClusterPair` constructor

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
func (pc *PrimaryCluster) ServerIterator(ctx context.Context) (<-chan ardb.StorageServer, error) {
	pc.mux.Lock()
	ch := make(chan ardb.StorageServer)
	go func() {
		defer pc.mux.Unlock()
		defer close(ch)

		for index := int64(0); index < pc.serverCount; index++ {
			operational, _ := pc.serverOperational(index)
			if !operational {
				continue
			}

			server := primaryStorageServer{
				index:   index,
				cluster: pc,
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
func (pc *PrimaryCluster) ServerCount() int64 {
	pc.mux.RLock()

	count := pc.serverCount
	for _, server := range pc.servers {
		if server.State != config.StorageServerStateOnline {
			count--
		}
	}
	pc.mux.RUnlock()

	return count
}

// execute an exuction at a given primary server
func (pc *PrimaryCluster) doAt(serverIndex int64, cfg config.StorageServerConfig, action ardb.StorageAction) (reply interface{}, err error) {
	// establish a connection for the given config
	conn, err := pc.pool.Dial(cfg)
	if err == nil {
		defer conn.Close()
		reply, err = action.Do(conn)
		if err == nil || errors.Cause(err) == ardb.ErrNil {
			return
		}
	}

	// TODO:
	// add self-healing...
	// see: https://github.com/zero-os/0-Disk/issues/445
	// and  https://github.com/zero-os/0-Disk/issues/284

	// an error has occured, broadcast it to AYS
	status := MapErrorToBroadcastStatus(err)
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
	var primaryWatcher ClusterConfigWatcher
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

// primaryStorageServer defines a primary storage server.
type primaryStorageServer struct {
	index   int64
	cluster *PrimaryCluster
}

// Do implements StorageServer.Do
func (server primaryStorageServer) Do(action ardb.StorageAction) (reply interface{}, err error) {
	cfg := server.cluster.servers[server.index]
	return server.cluster.doAt(server.index, cfg, action)
}

// Config implements StorageServer.Config
func (server primaryStorageServer) Config() config.StorageServerConfig {
	return server.cluster.servers[server.index]
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

	// deprecated
	_ ardb.StorageCluster = (*PrimaryCluster)(nil)
)

// enforces that our ServerIterators
// are actually ServerIterators
var (
	_ ardb.StorageServer = smartServer{}

	// deprecated
	_ ardb.StorageServer = primaryStorageServer{}
)

// enforces that our ClusterStateControllers
// are actually ClusterStateControllers
var (
	_ ClusterStateController = (*primarySlaveClusterPairStateController)(nil)
	_ ClusterStateController = (*templateClusterStateController)(nil)
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
)

var (
	// an error used as a dummy error,
	// it should never be returned
	errActionNotApplied = errors.New("storage action not applied")
)
