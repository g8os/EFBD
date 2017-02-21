package main

import (
	"log"
	"os"
	"sync"

	"github.com/abligh/gonbdserver/nbd"
	"golang.org/x/net/context"
)

func main() {
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
		Protocol: "tcp",
		Address:  ":6666",
		Exports: []nbd.ExportConfig{nbd.ExportConfig{
			Name:        "default",
			Description: "Deduped g8os blockstor",
			Driver:      "ardb",
			Workers:     5,
		}},
	}
	l, err := nbd.NewListener(logger, s)
	if err != nil {
		logger.Fatal(err)
		return
	}
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
