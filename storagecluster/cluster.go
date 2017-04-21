package storagecluster

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	log "github.com/glendc/go-mini-log"

	gridapi "github.com/g8os/blockstor/gridapi/gridapiclient"
)

// NewClusterClientFactory creates a ClusterClientFactory.
func NewClusterClientFactory(gridapiaddress string, logger log.Logger) (*ClusterClientFactory, error) {
	if gridapiaddress == "" {
		return nil, errors.New("NewClusterClientFactory requires a non-empty gridapiaddress")
	}
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	return &ClusterClientFactory{
		gridapiaddress: gridapiaddress,
		requestCh:      make(chan string),
		responseCh:     make(chan clusterClientResponse),
		logger:         logger,
	}, nil
}

// ClusterClientFactory allows for the creation of ClusterClients.
type ClusterClientFactory struct {
	gridapiaddress string
	// used for creation of storage cluster clients
	requestCh  chan string
	responseCh chan clusterClientResponse
	// used for internal logging purposes
	logger log.Logger
}

// NewClient returns a new ClusterClient.
func (f *ClusterClientFactory) NewClient(vdiskID string) (cc *ClusterClient, err error) {
	if vdiskID == "" {
		err = errors.New("ClusterClient requires a non-empty vdiskID")
		return
	}

	f.requestCh <- vdiskID
	resp := <-f.responseCh

	cc = resp.Client
	err = resp.Error
	return
}

// Listen to incoming creation requests (send by the NewClient method)
func (f *ClusterClientFactory) Listen(ctx context.Context) {
	for {
		select {
		// wait for a request
		case vdiskID := <-f.requestCh:
			cc, err := NewClusterClient(
				ClusterClientConfig{
					GridAPIAddress: f.gridapiaddress,
					VdiskID:        vdiskID,
				},
				f.logger,
			)
			if err != nil {
				// couldn't create cc, early exit
				f.responseCh <- clusterClientResponse{Error: err}
				continue
			}

			cc.done = make(chan struct{}, 1)
			go cc.listen(ctx)

			// all fine, return the configuration
			f.responseCh <- clusterClientResponse{Client: cc}

		// or until the context is done
		case <-ctx.Done():
			return
		}
	}
}

type clusterClientResponse struct {
	Client *ClusterClient
	Error  error
}

// ClusterClientConfig contains all configurable parameters
// used when creating a ClusterClient
type ClusterClientConfig struct {
	GridAPIAddress     string
	VdiskID            string
	StorageClusterName string
}

// NewClusterClient creates a new cluster client
func NewClusterClient(cfg ClusterClientConfig, logger log.Logger) (*ClusterClient, error) {
	client := gridapi.NewG8OSStatelessGRID()
	client.BaseURI = cfg.GridAPIAddress

	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	cc := &ClusterClient{
		client:             client,
		vdiskID:            cfg.VdiskID,
		storageClusterName: cfg.StorageClusterName,
		logger:             logger,
		done:               make(chan struct{}, 1),
	}

	if !cc.loadConfig() {
		return nil, errors.New("couldn't load configuration")
	}

	return cc, nil
}

// ClusterClient contains the cluster configuration,
// which gets reloaded based on incoming SIGHUP signals.
type ClusterClient struct {
	client *gridapi.G8OSStatelessGRID

	// when storageClusterName is given,
	// vdiskID isn't needed and thus not used
	vdiskID, storageClusterName string

	// used to log
	logger log.Logger

	// keep type, such that we can check this,
	// when reloading the configuration
	vdiskType gridapi.EnumVdiskType

	// used to get a redis connection
	servers         []gridapi.HAStorageServer
	numberOfServers int64 //Keep it as a seperate variable since this is constantly needed

	// used to store meta data
	metaConnectionString string

	// indicates if configuration is succesfully loaded
	loaded bool

	// mutex
	mux sync.Mutex

	// used to stop the listening
	done chan struct{}
}

// ConnectionString returns a connectionstring,
// based on a given index, which will be morphed into a local index,
// based on the available (local) storage servers available.
func (cc *ClusterClient) ConnectionString(index int64) (string, error) {
	cc.mux.Lock()
	defer cc.mux.Unlock()

	if !cc.loaded && !cc.loadConfig() {
		return "", errors.New("couldn't load storage cluster config")
	}

	bcIndex := index % cc.numberOfServers
	return connectionStringFromHAStorageServer(cc.servers[bcIndex]), nil
}

// MetaConnectionString returns the connectionstring (`<host>:<port>`),
// used to connect to the meta storage server.
func (cc *ClusterClient) MetaConnectionString() (string, error) {
	cc.mux.Lock()
	defer cc.mux.Unlock()

	if !cc.loaded && !cc.loadConfig() {
		return "", errors.New("couldn't load storage cluster config")
	}

	return cc.metaConnectionString, nil
}

// Close the open listen goroutine,
// which autoreloads the internal configuration,
// upon receiving a SIGHUP signal.
func (cc *ClusterClient) Close() {
	if cc.done != nil {
		cc.done <- struct{}{}
		close(cc.done)
		cc.done = nil
	}
}

// listen to incoming signals,
// and reload configuration when receiving a SIGHUP signal.
func (cc *ClusterClient) listen(ctx context.Context) {
	cc.logger.Info("ready to reload StorageClusterConfig upon SIGHUP receival for:", cc.vdiskID)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	defer signal.Stop(ch)

	for {
		select {
		case s := <-ch:
			switch s {
			case syscall.SIGHUP:
				cc.logger.Infof("%q received SIGHUP Signal", cc.vdiskID)
				func() {
					cc.mux.Lock()
					defer cc.mux.Unlock()
					cc.loadConfig()
				}()
			default:
				cc.logger.Info("received unsupported signal", s)
			}

		case <-cc.done:
			cc.logger.Info(
				"exit listener for StorageClusterConfig for vdisk:",
				cc.vdiskID)
			return

		case <-ctx.Done():
			cc.logger.Info(
				"abort listener for StorageClusterConfig for vdisk:",
				cc.vdiskID)
			return
		}
	}
}

func (cc *ClusterClient) loadConfig() bool {
	cc.loaded = false

	cc.logger.Info("loading storage cluster config")

	var storageClusterName string

	if cc.storageClusterName == "" && cc.vdiskID != "" {
		// get vdisk info
		vdiskInfo, _, err := cc.client.Vdisks.GetVdiskInfo(cc.vdiskID, nil, nil)
		if err != nil {
			cc.logger.Infof("couldn't get vdiskInfo: %s", err.Error())
			return false
		}

		// check vdiskType, and sure it's the same one as last time
		if cc.vdiskType != "" && cc.vdiskType != vdiskInfo.Type {
			cc.logger.Infof("wrong type for vdisk %q, expected %q, while received %q",
				cc.vdiskID, cc.vdiskType, vdiskInfo.Type)
			return false
		}
		cc.vdiskType = vdiskInfo.Type

		storageClusterName = vdiskInfo.Storagecluster
	} else if cc.storageClusterName != "" {
		cc.logger.Infof(
			"skipping fetching vdiskInfo because storage cluster name (%s) is already given",
			cc.storageClusterName)
		storageClusterName = cc.storageClusterName
	} else {
		cc.logger.Info("couldn't load config: either the volumeID or the storageClusterName has to be defined")
		return false
	}

	//Get information about the backend storage nodes
	storageClusterInfo, _, err :=
		cc.client.Storageclusters.GetClusterInfo(storageClusterName, nil, nil)
	if err != nil {
		cc.logger.Infof("couldn't get storage cluster info: %s", err.Error())
		return false
	}

	// store information required for getting redis connections
	cc.servers = storageClusterInfo.DataStorage
	cc.numberOfServers = int64(len(cc.servers))
	if cc.numberOfServers < 1 {
		cc.logger.Info(
			"received no storageBackendController, while at least 1 is required")
		return false
	}

	// used to store metadata
	if len(storageClusterInfo.MetadataStorage) < 1 {
		cc.logger.Infof("No metadata servers available in storagecluster %s", storageClusterName)
		return false
	}
	cc.metaConnectionString = connectionStringFromHAStorageServer(storageClusterInfo.MetadataStorage[0])

	cc.loaded = true
	return cc.loaded
}

func connectionStringFromHAStorageServer(server gridapi.HAStorageServer) string {
	return server.Master.Ip + ":" + strconv.Itoa(server.Master.Port)
}
