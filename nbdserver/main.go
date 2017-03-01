package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/abligh/gonbdserver/nbd"
	"golang.org/x/net/context"
)

func main() {
	var inMemoryStorage bool
	var protocol string
	var address string
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.Parse()

	logger := log.New(os.Stderr, "nbdserver:", log.Ldate|log.Ltime)
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
		Protocol: protocol,
		Address:  address,
		Exports: []nbd.ExportConfig{nbd.ExportConfig{
			Name:             "default",
			Description:      "Deduped g8os blockstor",
			Driver:           "ardb",
			Workers:          5,
			DriverParameters: map[string]string{"ardbimplementation": "external"},
		}},
	}
	if inMemoryStorage {
		s.Exports[0].DriverParameters["ardbimplementation"] = "inmemory"
	}
	log.Println(s.Exports[0].DriverParameters["ardbimplementation"])
	l, err := nbd.NewListener(logger, s)
	if err != nil {
		logger.Fatal(err)
		return
	}
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
