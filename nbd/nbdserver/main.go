package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "net/http/pprof"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog"
	tlogserver "github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

func main() {
	var inMemoryStorage bool
	var tlsonly bool
	var verbose bool
	var lbacachelimit int64
	var profileAddress string
	var protocol string
	var address string
	var tlogrpcaddress string
	var configPath string
	var logPath string
	flag.BoolVar(&verbose, "v", false, "when false, only log warnings and errors")
	flag.StringVar(&logPath, "logfile", "", "optionally log to the specified file, instead of the stderr")
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.BoolVar(&tlsonly, "tlsonly", false, "Forces all nbd connections to be tls-enabled")
	flag.StringVar(&profileAddress, "profile-address", "", "Enables profiling of this server as an http service")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.StringVar(&tlogrpcaddress, "tlogrpc", "", "Addresses of the tlog server, set to 'auto' to use the inmemory version (test/dev only)")
	flag.StringVar(&configPath, "config", "config.yml", "NBDServer Config YAML File")
	flag.Int64Var(&lbacachelimit, "lbacachelimit", ardb.DefaultLBACacheLimit,
		fmt.Sprintf("Cache limit of LBA in bytes, needs to be higher then %d (bytes in 1 sector)", lba.BytesPerSector))
	flag.Parse()

	logLevel := log.InfoLevel
	if verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	var logHandlers []log.Handler

	if logPath != "" {
		handler, err := log.FileHandler(logPath)
		if err != nil {
			log.Fatal(err)
		}
		logHandlers = append(logHandlers, handler)
		log.SetHandlers(logHandlers...)
	}

	log.Debugf("flags parsed: memorystorage=%t tlsonly=%t profileaddress=%q protocol=%q address=%q tlogrpc=%q config=%q lbacachelimit=%d logfile=%q",
		inMemoryStorage, tlsonly,
		profileAddress,
		protocol, address,
		tlogrpcaddress,
		configPath,
		lbacachelimit,
		logPath,
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

	ctx, cancelFunc := context.WithCancel(context.Background())
	// create embedded tlog api if needed
	if tlogrpcaddress == "auto" {
		log.Info("Starting embedded (in-memory) tlogserver")
		config := tlogserver.DefaultConfig()

		var err error
		var poolFactory tlog.RedisPoolFactory

		requiredDataServers := config.RequiredDataServers()

		if inMemoryStorage {
			poolFactory = tlog.InMemoryRedisPoolFactory(requiredDataServers)
		} else {
			poolFactory, err = tlog.ConfigRedisPoolFactory(ctx, requiredDataServers, configPath)
			if err != nil {
				log.Fatalf("couldn't create embedded tlogserver: %v", err)
			}
		}

		// create server
		server, err := tlogserver.NewServer(config, poolFactory)
		if err != nil {
			log.Fatalf("couldn't create embedded tlogserver: %v", err)
		}

		tlogrpcaddress = server.ListenAddr()

		log.Debug("embedded (in-memory) tlogserver up and running")
		go server.Listen(ctx)
	}
	if tlogrpcaddress != "" {
		log.Info("Using tlog server at", tlogrpcaddress)
	}

	var sessionWaitGroup sync.WaitGroup

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

	// Only when we want to use inmemory (ledis) storage (for dev purposes),
	// do we define a poolDial func, which will be used in the redis pool.
	// In production no dial function is defined,
	// hence the redis pool will simply dial a TCP connection using
	// a given address and database number.
	var poolDial ardb.DialFunc
	if inMemoryStorage {
		log.Info("Using in-memory block storage")
		memoryRedis := redisstub.NewMemoryRedis()
		go memoryRedis.Listen()
		defer memoryRedis.Close()
		poolDial = memoryRedis.Dial
	}

	// A pool factory is used,
	// such that each Vdisk can get its own redis pool,
	// rather than having a shared redis pool
	// between all vdisks ever...
	redisPoolFactory := ardb.NewRedisPoolFactory(poolDial)

	configHotReloader, err := config.NewHotReloader(configPath, config.NBDServer)
	if err != nil {
		log.Fatal(err)
	}
	go configHotReloader.Listen(configCtx)
	defer configHotReloader.Close()

	backendFactory, err := newBackendFactory(backendFactoryConfig{
		PoolFactory:       redisPoolFactory,
		ConfigHotReloader: configHotReloader,
		TLogRPCAddress:    tlogrpcaddress,
		LBACacheLimit:     lbacachelimit,
		ConfigPath:        configPath,
	})
	handleSigterm(backendFactory, cancelFunc)

	if err != nil {
		log.Fatal(err)
	}

	nbd.RegisterBackend("ardb", backendFactory.NewBackend)

	l, err := nbd.NewListener(log.New("nbdserver", logLevel, logHandlers...), s)
	if err != nil {
		log.Fatal(err)
		return
	}

	exportController, err := NewExportController(
		configHotReloader,
		tlsonly,
	)
	if err != nil {
		log.Fatal(err)
	}

	// set export config controller,
	// so we can generate the ExportConfig,
	// dynamically based on the given vdisk
	l.SetExportConfigManager(exportController)

	// listen to requests
	l.Listen(configCtx, ctx, &sessionWaitGroup)
}

// handle sigterm
// - wait for all vdisks (that need to be waited) completion
// - log vdisk completion error to stderr
func handleSigterm(bf *backendFactory, cancelFunc context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs

		errs := bf.Wait()
		for _, err := range errs {
			// TODO : log to stderr properly
			// depends on : https://github.com/zero-os/0-Disk/issues/300
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}

		// stop other vdisks
		cancelFunc()

		os.Exit(1)
	}()

}

func init() {
	flag.Usage = func() {
		var exe string
		if len(os.Args) > 0 {
			exe = os.Args[0]
		} else {
			exe = "nbdserver"
		}

		fmt.Fprintln(os.Stderr, "nbdserver", zerodisk.CurrentVersion)
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Usage of", exe+":")
		flag.PrintDefaults()
	}
}
