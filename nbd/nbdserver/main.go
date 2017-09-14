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
)

func main() {
	var inMemoryStorage bool
	var tlsonly bool
	var verbose bool
	var lbacachelimit int64
	var profileAddress string
	var protocol string
	var address string
	var sourceConfig config.SourceConfig
	var logPath string
	var serverID string
	flag.BoolVar(&verbose, "v", false, "when false, only log warnings and errors")
	flag.StringVar(&logPath, "logfile", "", "optionally log to the specified file, instead of the stderr")
	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the data in memory only, usefull for testing or benchmarking")
	flag.BoolVar(&tlsonly, "tlsonly", false, "Forces all nbd connections to be tls-enabled")
	flag.StringVar(&profileAddress, "profile-address", "", "Enables profiling of this server as an http service")
	flag.StringVar(&protocol, "protocol", "unix", "Protocol to listen on, 'tcp' or 'unix'")
	flag.StringVar(&address, "address", "/tmp/nbd-socket", "Address to listen on, unix socket or tcp address, ':6666' for example")
	flag.Var(&sourceConfig, "config", "config resource: dialstrings (etcd cluster) or path (yaml file)")
	flag.Int64Var(&lbacachelimit, "lbacachelimit", ardb.DefaultLBACacheLimit,
		fmt.Sprintf("Cache limit of LBA in bytes, needs to be higher then %d (bytes in 1 sector)", lba.BytesPerSector))
	flag.StringVar(&serverID, "id", "default", "The server ID (default: default)")
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

	log.Debugf("flags parsed: memorystorage=%t tlsonly=%t profileaddress=%q protocol=%q address=%q config=%q lbacachelimit=%d logfile=%q id=%q",
		inMemoryStorage, tlsonly,
		profileAddress,
		protocol, address,
		sourceConfig.String(),
		lbacachelimit,
		logPath,
		serverID,
	)

	// let's create the source and defer close it
	configSource, err := config.NewSource(sourceConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer configSource.Close()
	// let's now also ensure that the configuration is valid
	err = config.ValidateNBDServerConfigs(configSource, serverID)
	if err != nil {
		log.Fatal(err)
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

	ctx, cancelFunc := context.WithCancel(context.Background())

	var sessionWaitGroup sync.WaitGroup

	defer func() {
		log.Info("Shutting down")
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

	backendFactory, err := newBackendFactory(backendFactoryConfig{
		PoolFactory:   redisPoolFactory,
		ConfigSource:  configSource,
		LBACacheLimit: lbacachelimit,
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
		ctx,
		configSource,
		tlsonly,
		serverID,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer exportController.Close()

	// set export config controller,
	// so we can generate the ExportConfig,
	// dynamically based on the given vdisk
	l.SetExportConfigManager(exportController)

	// listen to requests
	l.Listen(ctx, ctx, &sessionWaitGroup)
}

// handle sigterm
// - wait for all vdisks (that need to be waited) completion
// - log vdisk completion error to stderr
func handleSigterm(bf *backendFactory, cancelFunc context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Info("nbd server received SIGTERM")

		errs := bf.StopAndWait()
		if len(errs) == 0 {
			log.Info("nbd server SIGTERM handler finished with no error")
		}
		for _, err := range errs {
			log.Infof("sigterm handler got error: %v", err)
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
		fmt.Fprintln(os.Stderr, fmt.Sprintf("usage: %s [flags] config_resource", exe))
		flag.PrintDefaults()
	}
}
