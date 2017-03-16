package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"

	_ "net/http/pprof"

	"github.com/g8os/blockstor/nbdserver/arbd"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/nbdserver/stubs"
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
	var volumecontrolleraddress string
	var backendcontrolleraddress string
	var testArdbConnectionSrings string
	var defaultExport string
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.BoolVar(&tslonly, "tslonly", false, "Forces all nbd connections to be tsl-enabled")
	flag.StringVar(&profileAddress, "profileaddress", "", "Enables profiling of this server as an http service")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.StringVar(&volumecontrolleraddress, "volumecontroller", "", "Address of the volumecontroller REST API, leave empty to use the embedded stub")
	flag.StringVar(&backendcontrolleraddress, "backendcontroller", "", "Address of the storage backend controller REST API, leave empty to use the embedded stub")
	flag.StringVar(&testArdbConnectionSrings, "testardbs", "localhost:16379,localhost:16379", "Comma seperated list of ardb connection strings returned by the embedded backend controller, first one is the metadataserver")
	flag.StringVar(&defaultExport, "export", "default", "default export to list and use")
	flag.Int64Var(&lbacachelimit, "lbacachelimit", arbd.DefaultLBACacheLimit,
		fmt.Sprintf("Cache limit of LBA in bytes, needs to be higher then %d (bytes in 1 shard)", lba.BytesPerShard))
	flag.Parse()

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

	if volumecontrolleraddress == "" {
		logger.Println("[INFO] Starting embedded volume controller")
		var s *httptest.Server
		s, volumecontrolleraddress = stubs.NewVolumeControllerServer()
		defer s.Close()
	}
	logger.Println("[INFO] Using volume controller at", volumecontrolleraddress)

	if backendcontrolleraddress == "" {
		logger.Println("[INFO] Starting embedded storage backend controller")
		var s *httptest.Server
		var err error
		s, backendcontrolleraddress, err = stubs.NewStorageBackendServer(testArdbConnectionSrings)
		if err != nil {
			logger.Fatalln("[ERROR]", err)
		}
		defer s.Close()
	}
	logger.Println("[INFO] Using storage backend controller at", backendcontrolleraddress)

	exportController, err := NewExportController(
		volumecontrolleraddress,
		defaultExport,
		tslonly,
	)
	if err != nil {
		logger.Panicln(err)
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
		DefaultExport: defaultExport,
	}
	if inMemoryStorage {
		logger.Println("[INFO] Using in-memory block storage")
	}

	redisPool := arbd.NewRedisPool(inMemoryStorage)
	defer redisPool.Close()

	f := &arbd.BackendFactory{
		BackendPool:              redisPool,
		VolumeControllerAddress:  volumecontrolleraddress,
		BackendControllerAddress: backendcontrolleraddress,
		LBACacheLimit:            lbacachelimit,
	}

	nbd.RegisterBackend("ardb", f.NewBackend)

	l, err := nbd.NewListener(logger, s)
	if err != nil {
		logger.Fatal(err)
		return
	}

	// set export config controller,
	// so we can generate the ExportConfig,
	// dynamically based on the given volume
	l.SetExportConfigManager(exportController)

	// listen to requests
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
