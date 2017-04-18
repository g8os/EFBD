package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"

	_ "net/http/pprof"

	log "github.com/glendc/go-mini-log"

	"github.com/g8os/blockstor/gridapi"
	"github.com/g8os/blockstor/nbdserver/ardb"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/redisstub"
	"github.com/g8os/blockstor/storagecluster"
	"github.com/g8os/gonbdserver/nbd"
)

func main() {
	var inMemoryStorage bool
	var tslonly bool
	var verbose bool
	var lbacachelimit int64
	var profileAddress string
	var protocol string
	var address string
	var gridapiaddress string
	var testArdbConnectionSrings string
	var exports string
	var nonDedupedExports string
	flag.BoolVar(&verbose, "v", false, "when false, only log warnings and errors")
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

	logFlags := log.Ldate | log.Ltime | log.Lshortfile

	if verbose {
		logFlags |= log.LDebug
	}

	log.SetFlags(logFlags)
	log.Debugf("flags parsed: memorystorage=%t tslonly=%t profileaddress=%q protocol=%q address=%q gridapi=%q nondeduped=%q testardbs=%q export=%q lbacachelimit=%d",
		inMemoryStorage, tslonly,
		profileAddress,
		protocol, address,
		gridapiaddress,
		nonDedupedExports,
		testArdbConnectionSrings,
		exports,
		lbacachelimit)

	exportArray := strings.Split(exports, ",")
	if len(exportArray) == 0 {
		exportArray = []string{"default"}
	}

	if len(profileAddress) > 0 {
		go func() {
			log.Info("profiling enabled, available on", profileAddress)
			err := http.ListenAndServe(profileAddress, http.DefaultServeMux)
			if err != nil {
				log.Info("profiler couldn't be started:", err)
			}
		}()
	}

	if gridapiaddress == "" {
		log.Info("Starting embedded grid api")
		var s *httptest.Server
		var err error
		s, gridapiaddress, err = gridapi.NewGridAPIServer(testArdbConnectionSrings, strings.Split(nonDedupedExports, ","))
		if err != nil {
			log.Fatal(err)
		}
		defer s.Close()
	}
	log.Info("Using grid api at", gridapiaddress)

	exportController, err := NewExportController(
		gridapiaddress,
		tslonly,
		exportArray,
	)
	if err != nil {
		log.Fatal(err)
	}

	var sessionWaitGroup sync.WaitGroup

	ctx, cancelFunc := context.WithCancel(context.Background())
	configCtx, _ := context.WithCancel(ctx)
	defer func() {
		log.Info("Shutting down")
		cancelFunc()
		sessionWaitGroup.Wait()
		log.Info("Shutdown complete")
	}()
	s := nbd.ServerConfig{
		Protocol:      protocol,
		Address:       address,
		DefaultExport: exportArray[0],
	}

	var poolDial ardb.DialFunc
	if inMemoryStorage {
		log.Info("Using in-memory block storage")
		memoryRedis := redisstub.NewMemoryRedis()
		go memoryRedis.Listen()
		defer memoryRedis.Close()
		poolDial = memoryRedis.Dial
	}

	redisPool := ardb.NewRedisPool(poolDial)
	defer redisPool.Close()

	storageClusterCfgFactory, err := storagecluster.NewClusterConfigFactory(gridapiaddress)
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}

	nbd.RegisterBackend("ardb", backendFactory.NewBackend)

	l, err := nbd.NewListener(log.New(os.Stderr, "nbdserver:", logFlags), s)
	if err != nil {
		log.Fatal(err)
		return
	}

	// set export config controller,
	// so we can generate the ExportConfig,
	// dynamically based on the given volume
	l.SetExportConfigManager(exportController)

	// listen to requests
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
