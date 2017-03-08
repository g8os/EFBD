package main

import (
	"flag"
	"log"
	"net/http/httptest"
	"os"
	"sync"

	"github.com/abligh/gonbdserver/nbd"
	"github.com/g8os/blockstor/nbdserver/stubs"
	"golang.org/x/net/context"
)

func main() {
	var inMemoryStorage bool
	var protocol string
	var address string
	var volumecontrolleraddress string
	var backendcontrolleraddress string
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.StringVar(&volumecontrolleraddress, "volumecontroller", "", "Address of the volumecontroller REST API, leave empty to use the embedded stub")
	flag.StringVar(&backendcontrolleraddress, "backendcontroller", "", "Address of the storage backend controller REST API, leave empty to use the embedded stub")
	flag.Parse()

	//TODO: make this dependant of a profiling flag
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", http.HandlerFunc(pprof.Index)))
	// }()

	logger := log.New(os.Stderr, "nbdserver:", log.Ldate|log.Ltime)
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
		s, backendcontrolleraddress = stubs.NewStorageBackendServer()
		defer s.Close()
	}
	logger.Println("[INFO] Using storage backend controller at", backendcontrolleraddress)

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
		DefaultExport: "default",
		Exports: []nbd.ExportConfig{nbd.ExportConfig{
			Name:        "default",
			Description: "Deduped g8os blockstor",
			Driver:      "ardb",
			Workers:     5,
			DriverParameters: map[string]string{
				"volumecontrolleraddress":  volumecontrolleraddress,
				"backendcontrolleraddress": backendcontrolleraddress,
			},
		}},
	}
	if inMemoryStorage {
		logger.Println("[INFO] Using in-memory block storage")
	}
	f := &ArdbBackendFactory{BackendPool: NewRedisPool(inMemoryStorage)}

	nbd.RegisterBackend("ardb", f.NewArdbBackend)

	l, err := nbd.NewListener(logger, s)
	if err != nil {
		logger.Fatal(err)
		return
	}
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
