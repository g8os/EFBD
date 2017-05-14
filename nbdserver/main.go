package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"sync"

	_ "net/http/pprof"

	"github.com/g8os/blockstor/log"

	"github.com/g8os/blockstor/gonbdserver/nbd"
	"github.com/g8os/blockstor/nbdserver/ardb"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/redisstub"
	"github.com/g8os/blockstor/storagecluster"
)

func main() {
	var inMemoryStorage bool
	var tlsonly bool
	var verbose bool
	var lbacachelimit int64
	var profileAddress string
	var protocol string
	var address string
	var configPath string
	var logPath string
	var syslogTag string
	flag.BoolVar(&verbose, "v", false, "when false, only log warnings and errors")
	flag.StringVar(&logPath, "logfile", "", "optionally log everything also to the specified file")
	flag.StringVar(&syslogTag, "syslog", "", "optionally log everything also to the system log")
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.BoolVar(&tlsonly, "tlsonly", false, "Forces all nbd connections to be tls-enabled")
	flag.StringVar(&profileAddress, "profileaddress", "", "Enables profiling of this server as an http service")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.StringVar(&configPath, "config", "config.yml", "ARDB Config YAML File")
	flag.Int64Var(&lbacachelimit, "lbacachelimit", ardb.DefaultLBACacheLimit,
		fmt.Sprintf("Cache limit of LBA in bytes, needs to be higher then %d (bytes in 1 shard)", lba.BytesPerShard))
	flag.Parse()

	logLevel := log.InfoLevel
	if verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	var logHandlers []log.Handler

	if syslogTag != "" {
		handler, err := log.SyslogHandler(syslogTag)
		if err != nil {
			log.Fatal(err)
		}
		logHandlers = append(logHandlers, handler)
	}
	if logPath != "" {
		handler, err := log.FileHandler(logPath)
		if err != nil {
			log.Fatal(err)
		}
		logHandlers = append(logHandlers, handler)
	}

	log.SetHandlers(logHandlers...)

	log.Debugf("flags parsed: memorystorage=%t tlsonly=%t profileaddress=%q protocol=%q address=%q config=%q lbacachelimit=%d logfile=%q syslog=%q",
		inMemoryStorage, tlsonly,
		profileAddress,
		protocol, address,
		configPath,
		lbacachelimit,
		logPath, syslogTag,
	)

	if len(profileAddress) > 0 {
		go func() {
			log.Info("profiling enabled, available on", profileAddress)
			err := http.ListenAndServe(profileAddress, http.DefaultServeMux)
			if err != nil {
				log.Info("profiler couldn't be started:", err)
			}
		}()
	}

	exportController, err := NewExportController(
		configPath,
		tlsonly,
	)
	if err != nil {
		log.Fatal(err)
	}

	var sessionWaitGroup sync.WaitGroup

	ctx, cancelFunc := context.WithCancel(context.Background())
	configCtx, configCancelFunc := context.WithCancel(ctx)
	defer func() {
		log.Info("Shutting down")
		configCancelFunc()
		cancelFunc()
		sessionWaitGroup.Wait()
		log.Info("Shutdown complete")
	}()

	s := nbd.ServerConfig{
		Protocol:      protocol,
		Address:       address,
		DefaultExport: "", // no default export is useful for our usecase
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

	storageClusterClientFactory, err := storagecluster.NewClusterClientFactory(
		configPath, log.New("storagecluster", logLevel, logHandlers...))
	if err != nil {
		log.Fatal(err)
	}

	// listens to incoming requests to create a dynamic StorageClusterConfig,
	// this is run on a goroutine, such that it can create
	// internal listeners as a goroutine
	go storageClusterClientFactory.Listen(configCtx)

	backendFactory, err := ardb.NewBackendFactory(ardb.BackendFactoryConfig{
		Pool:            redisPool,
		SCClientFactory: storageClusterClientFactory,
		ConfigPath:      configPath,
		LBACacheLimit:   lbacachelimit,
	})
	if err != nil {
		log.Fatal(err)
	}

	nbd.RegisterBackend("ardb", backendFactory.NewBackend)

	l, err := nbd.NewListener(log.New("nbdserver", logLevel, logHandlers...), s)
	if err != nil {
		log.Fatal(err)
		return
	}

	// set export config controller,
	// so we can generate the ExportConfig,
	// dynamically based on the given vdisk
	l.SetExportConfigManager(exportController)

	// listen to requests
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}
