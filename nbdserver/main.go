package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"

	_ "net/http/pprof"

	"github.com/g8os/blockstor/gridapi"
	"github.com/g8os/blockstor/nbdserver/ardb"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/redisstub"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/gonbdserver/nbd"
	"golang.org/x/net/context"
)

func main() {
	var inMemoryStorage bool
	var tslonly bool
	var lbacachelimit int64
	var profileAddress string
	var protocol string
	var address string
	var gridapiaddress string
	var testArdbConnectionSrings string
	var exports string
	var nonDedupedExports string
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.BoolVar(&tslonly, "tslonly", false, "Forces all nbd connections to be tls-enabled")
	flag.StringVar(&profileAddress, "profileaddress", "", "Enables profiling of this server as an http service")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.StringVar(&gridapiaddress, "gridapi", "", "Address of the grid api REST API, leave empty to use the embedded stub")
	flag.StringVar(&nonDedupedExports, "nondeduped", "", "when using the embedded volumecontroller, comma seperated list of exports that should not be deduped")
	flag.StringVar(&testArdbConnectionSrings, "testardbs", "localhost:16379,localhost:16379", "Comma seperated list of ardb connection strings returned by the embedded backend controller, first one is the metadataserver")
	flag.StringVar(&exports, "export", "default", "comma seperated list of exports to list and use")
	flag.Int64Var(&lbacachelimit, "lbacachelimit", ardb.DefaultLBACacheLimit,
		fmt.Sprintf("Cache limit of LBA in bytes, needs to be higher then %d (bytes in 1 shard)", lba.BytesPerShard))
	flag.Parse()

	exportArray := strings.Split(exports, ",")
	if len(exportArray) == 0 {
		exportArray = []string{"default"}
	}

	logger := log.New(os.Stderr, "nbdserver:", log.Ldate|log.Ltime)

	if len(profileAddress) > 0 {
		go func() {
			logger.Println("[INFO] profiling enabled, available on", profileAddress)
			err := http.ListenAndServe(profileAddress, http.DefaultServeMux)
			if err != nil {
				logger.Println("[ERROR] profiler couldn't be started:", err)
			}
		}()
	}

	if gridapiaddress == "" {
		logger.Println("[INFO] Starting embedded grid api")
		var s *httptest.Server
		var err error
		s, gridapiaddress, err = gridapi.NewGridAPIServer(testArdbConnectionSrings, strings.Split(nonDedupedExports, ","))
		if err != nil {
			logger.Fatalln("[ERROR]", err)
		}
		defer s.Close()
	}
	logger.Println("[INFO] Using grid api at", gridapiaddress)

	exportController, err := NewExportController(
		gridapiaddress,
		tslonly,
		exportArray,
	)
	if err != nil {
		logger.Fatalln("[ERROR]", err)
	}

	var sessionWaitGroup sync.WaitGroup

	ctx, cancelFunc := context.WithCancel(context.Background())
	configCtx, _ := context.WithCancel(ctx)
	defer func() {
		logger.Println("[INFO] Shutting down")
		cancelFunc()
		sessionWaitGroup.Wait()
		logger.Println("[INFO] Shutdown complete")
	}()
	s := nbd.ServerConfig{
		Protocol:      protocol,
		Address:       address,
		DefaultExport: exportArray[0],
	}

	var poolDial ardb.DialFunc
	if inMemoryStorage {
		logger.Println("[INFO] Using in-memory block storage")
		memoryRedis := redisstub.NewMemoryRedis()
		go memoryRedis.Listen()
		defer memoryRedis.Close()
		poolDial = memoryRedis.Dial
	}

	redisPool := ardb.NewRedisPool(poolDial)
	defer redisPool.Close()

	storageClusterCfgFactory, err := storagecluster.NewClusterConfigFactory(gridapiaddress)
	if err != nil {
		logger.Fatalln("[ERROR]", err)
	}

	// listens to incoming requests to create a dynamic StorageClusterConfig,
	// this is run on a goroutine, such that it can create
	// internal listeners as a goroutine
	go storageClusterCfgFactory.Listen(configCtx)

	backendFactory, err := ardb.NewBackendFactory(
		redisPool,
		storageClusterCfgFactory,
		gridapiaddress,
		lbacachelimit,
	)
	if err != nil {
		logger.Fatalln("[ERROR]", err)
	}

	nbd.RegisterBackend("ardb", backendFactory.NewBackend)

	l, err := nbd.NewListener(logger, s)
	if err != nil {
		logger.Fatalln("[ERROR]", err)
		return
	}

	// set export config controller,
	// so we can generate the ExportConfig,
	// dynamically based on the given volume
	l.SetExportConfigManager(exportController)

	// listen to requests
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
