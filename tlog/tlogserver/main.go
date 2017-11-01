package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

func main() {
	conf := server.DefaultConfig()

	var version bool
	var verbose bool
	var profileAddr string
	var storageAddresses string
	//var withSlaveSync bool
	var logPath string
	var sourceConfig config.SourceConfig
	var serverID string

	flag.StringVar(&conf.ListenAddr, "address", conf.ListenAddr, "Address to listen on")
	flag.IntVar(&conf.FlushSize, "flush-size", conf.FlushSize, "flush size")
	flag.IntVar(&conf.FlushTime, "flush-time", conf.FlushTime, "flush time (seconds)")
	flag.IntVar(&conf.BlockSize, "block-size", conf.BlockSize, "block size (bytes)")
	flag.StringVar(&conf.PrivKey, "priv-key", conf.PrivKey, "private key")
	flag.StringVar(&profileAddr, "profile-address", "", "Enables profiling of this server as an http service")
	flag.Var(&sourceConfig, "config", "config resource: dialstrings (etcd cluster) or path (yaml file)")
	//flag.BoolVar(&withSlaveSync, "with-slave-sync", false, "sync to ardb slave")
	flag.BoolVar(&verbose, "v", false, "log verbose (debug) statements")
	flag.StringVar(&logPath, "logfile", "", "optionally log to the specified file, instead of the stderr")
	flag.StringVar(&serverID, "id", "default", "The server ID (default: default)")
	flag.BoolVar(&version, "version", false, "prints build version and exits")

	flag.Parse()

	if version {
		zerodisk.PrintVersion()
		return
	}

	// config logger (verbose or not)
	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if logPath != "" {
		handler, err := log.FileHandler(logPath)
		if err != nil {
			log.Fatal(err)
		}
		log.SetHandlers(handler)
	}

	zerodisk.LogVersion()

	log.Debugf("flags parsed: address=%q flush-size=%d flush-time=%d block-size=%d priv-key=%q profile-address=%q config=%q storage-addresses=%q logfile=%q id=%q",
		conf.ListenAddr,
		conf.FlushSize,
		conf.FlushTime,
		conf.BlockSize,
		conf.PrivKey,
		profileAddr,
		sourceConfig.String(),
		storageAddresses,
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
	err = config.ValidateTlogServerConfigs(configSource, serverID)
	if err != nil {
		log.Fatal(err)
	}

	// profiling
	if profileAddr != "" {
		go func() {
			log.Infof("profiling enabled on %v", profileAddr)
			if err := http.ListenAndServe(profileAddr, http.DefaultServeMux); err != nil {
				log.Infof("Failed to enable profiling on %v, err:%v", profileAddr, err)
			}
		}()
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	// [TODO]
	// disabled until we work on a solution for
	// issue https://github.com/zero-os/0-Disk/issues/475,
	// as right now this feature writes to primary cluster which is not good for anything!
	// > This also disabled the slave sync stuff within the tlog vdisk code
	// > (due to conf.AggMq not being defined)
	/*if withSlaveSync {
		// aggregation MQ
		conf.AggMq = aggmq.NewMQ()

		// slave syncer manager
		ssm := slavesync.NewManager(ctx, conf.AggMq, configSource)
		go ssm.Run()
	}*/

	// create server
	server, err := server.NewServer(conf, configSource)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	server.Listen(ctx)
}

func init() {
	flag.Usage = func() {
		var exe string
		if len(os.Args) > 0 {
			exe = os.Args[0]
		} else {
			exe = "tlogserver"
		}

		fmt.Fprintln(os.Stderr, "tlogserver", zerodisk.CurrentVersion)
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, fmt.Sprintf("usage: %s [flags]", exe))
		flag.PrintDefaults()
	}
}
