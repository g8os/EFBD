package storagecluster

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	gridapi "github.com/g8os/blockstor/gridapi/gridapiclient"
	"golang.org/x/net/context"
)

// NewClusterConfigFactory creates a ClusterConfigFactory.
func NewClusterConfigFactory(gridapiaddress string) (*ClusterConfigFactory, error) {
	if gridapiaddress == "" {
		return nil, errors.New("NewClusterConfigFactory requires a non-empty gridapiaddress")
	}

	return &ClusterConfigFactory{
		gridapiaddress: gridapiaddress,
		requestCh:      make(chan string),
		responseCh:     make(chan clusterConfigResponse),
	}, nil
}

// ClusterConfigFactory allows for the creation of ClusterConfigs.
type ClusterConfigFactory struct {
	gridapiaddress string
	requestCh      chan string
	responseCh     chan clusterConfigResponse
}

// NewConfig returns a new ClusterConfig.
func (f *ClusterConfigFactory) NewConfig(volumeID string) (cfg *ClusterConfig, err error) {
	if volumeID == "" {
		err = errors.New("ClusterConfig requires a non-empty volumeID")
		return
	}

	f.requestCh <- volumeID
	resp := <-f.responseCh

	cfg = resp.Config
	err = resp.Error
	return
}

// Listen to incoming creation requests (send by the NewConfig method)
func (f *ClusterConfigFactory) Listen(ctx context.Context) {
	for {
		select {
		// wait for a request
		case volumeID := <-f.requestCh:
			cfg, err := newClusterConfig(
				f.gridapiaddress,
				volumeID,
				nil, // no logger
			)
			if err != nil {
				// couldn't create cfg, early exit
				f.responseCh <- clusterConfigResponse{Error: err}
				continue
			}

			cfg.done = make(chan struct{}, 1)
			go cfg.listen(ctx)

			// all fine, return the configuration
			f.responseCh <- clusterConfigResponse{Config: cfg}

		// or until the context is done
		case <-ctx.Done():
			return
		}
	}
}

type clusterConfigResponse struct {
	Config *ClusterConfig
	Error  error
}

// newClusterConfig creates a new cluster config
func newClusterConfig(gridapiaddress, volumeID string, logger *log.Logger) (*ClusterConfig, error) {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	client := gridapi.NewG8OSStatelessGRID()
	client.BaseURI = gridapiaddress

	cfg := &ClusterConfig{
		client:   client,
		logger:   logger,
		volumeID: volumeID,
		done:     make(chan struct{}, 1),
	}

	if !cfg.loadConfig() {
		return nil, errors.New("couldn't load configuration")
	}

	return cfg, nil
}

// ClusterConfig contains the cluster configuration,
// which gets reloaded based on incoming SIGHUP signals.
type ClusterConfig struct {
	client   *gridapi.G8OSStatelessGRID
	logger   *log.Logger
	volumeID string

	// keep type, such that we can check this,
	// when reloading the configuration
	volumeType gridapi.EnumVolumeVolumetype

	// used to get a redis connection
	servers         []gridapi.HAStorageServer
	numberOfServers int //Keep it as a seperate variable since this is constantly needed

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
// based on the available storage servers available.
func (cfg *ClusterConfig) ConnectionString(index int) (string, error) {
	cfg.mux.Lock()
	defer cfg.mux.Unlock()

	if !cfg.loaded && !cfg.loadConfig() {
		return "", errors.New("couldn't load storage cluster config")
	}

	bcIndex := index % cfg.numberOfServers
	return connectionStringFromHAStorageServer(cfg.servers[bcIndex]), nil
}

// MetaConnectionString returns the connectionstring (`<host>:<port>`),
// used to connect to the meta storage server.
func (cfg *ClusterConfig) MetaConnectionString() (string, error) {
	cfg.mux.Lock()
	defer cfg.mux.Unlock()

	if !cfg.loaded && !cfg.loadConfig() {
		return "", errors.New("couldn't load storage cluster config")
	}

	return cfg.metaConnectionString, nil
}

// Close the open listen goroutine,
// which autoreloads the internal configuration,
// upon receiving a SIGHUP signal.
func (cfg *ClusterConfig) Close() {
	if cfg.done != nil {
		cfg.done <- struct{}{}
		close(cfg.done)
		cfg.done = nil
	}
}

// listen to incoming signals,
// and reload configuration when receiving a SIGHUP signal.
func (cfg *ClusterConfig) listen(ctx context.Context) {
	cfg.logger.Println("[INFO] ready to reload StorageClusterConfig upon SIGHUP receival for:", cfg.volumeID)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	defer signal.Stop(ch)

	for {
		select {
		case s := <-ch:
			switch s {
			case syscall.SIGHUP:
				cfg.logger.Printf("[INFO] %q received SIGHUP Signal", cfg.volumeID)
				func() {
					cfg.mux.Lock()
					defer cfg.mux.Unlock()
					cfg.loadConfig()
				}()
			default:
				cfg.logger.Println("[WARN] received unsupported signal", s)
			}

		case <-cfg.done:
			cfg.logger.Println("[INFO] exit listener for StorageClusterConfig for volume:", cfg.volumeID)
			return

		case <-ctx.Done():
			cfg.logger.Println("[WARN] abort listener for StorageClusterConfig for volume:", cfg.volumeID)
			return
		}
	}
}

func (cfg *ClusterConfig) loadConfig() bool {
	cfg.loaded = false

	cfg.logger.Println("[INFO] loading storage cluster config")

	// get volume info
	volumeInfo, _, err := cfg.client.Volumes.GetVolumeInfo(cfg.volumeID, nil, nil)
	if err != nil {
		cfg.logger.Println("[ERROR]", err)
		return false
	}

	// check volumeType, and sure it's the same one as last time
	if cfg.volumeType != "" && cfg.volumeType != volumeInfo.Volumetype {
		cfg.logger.Printf(
			"[ERROR] wrong type for volume %q, expected %q, while received %q",
			cfg.volumeID, cfg.volumeType, volumeInfo.Volumetype)
		return false
	}
	cfg.volumeType = volumeInfo.Volumetype

	//Get information about the backend storage nodes
	storageClusterInfo, _, err := cfg.client.Storageclusters.GetClusterInfo(volumeInfo.Storagecluster, nil, nil)
	if err != nil {
		cfg.logger.Println("[ERROR]", err)
		return false
	}

	// store information required for getting redis connections
	cfg.servers = storageClusterInfo.DataStorage
	cfg.numberOfServers = len(cfg.servers)
	if cfg.numberOfServers < 1 {
		cfg.logger.Println("[ERROR] received no storageBackendController, while at least 1 is required")
		return false
	}

	// used to store metadata
	if len(storageClusterInfo.MetadataStorage) < 1 {
		cfg.logger.Printf("[ERROR] No metadata servers available in storagecluster %s", volumeInfo.Storagecluster)
		return false
	}
	cfg.metaConnectionString = connectionStringFromHAStorageServer(storageClusterInfo.MetadataStorage[0])

	cfg.loaded = true
	return cfg.loaded
}

func connectionStringFromHAStorageServer(server gridapi.HAStorageServer) string {
	return server.Master.Ip + ":" + strconv.Itoa(server.Master.Port)
}
