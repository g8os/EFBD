package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/tlogserver/aggmq"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
	"github.com/zero-os/0-Disk/tlog/tlogserver/slavesync"
)

func main() {
	conf := server.DefaultConfig()

	var verbose bool
	var profileAddr string
	var inMemoryStorage bool
	var storageAddresses string
	var withSlaveSync bool

	flag.StringVar(&conf.ListenAddr, "address", conf.ListenAddr, "Address to listen on")
	flag.IntVar(&conf.FlushSize, "flush-size", conf.FlushSize, "flush size")
	flag.IntVar(&conf.FlushTime, "flush-time", conf.FlushTime, "flush time (seconds)")
	flag.IntVar(&conf.BlockSize, "block-size", conf.BlockSize, "block size (bytes)")
	flag.IntVar(&conf.K, "k", conf.K, "K variable of the erasure encoding")
	flag.IntVar(&conf.M, "m", conf.M, "M variable of the erasure encoding")
	flag.StringVar(&conf.PrivKey, "priv-key", conf.PrivKey, "private key")
	flag.StringVar(&conf.HexNonce, "nonce", conf.HexNonce, "hex nonce used for encryption")
	flag.StringVar(&profileAddr, "profile-address", "", "Enables profiling of this server as an http service")

	flag.BoolVar(&inMemoryStorage, "memorystorage", false, "Stores the (meta)data in memory only, usefull for testing or benchmarking (overwrites the storage-addresses flag)")
	flag.StringVar(&conf.ConfigPath, "config", "config.yml", "Zerodisk Config YAML File")
	flag.StringVar(&storageAddresses, "storage-addresses", "",
		"comma seperated list of redis compatible connectionstrings (format: '<ip>:<port>[@<db>]', eg: 'localhost:16379,localhost:6379@2'), if given, these are used for all vdisks, ignoring the given config")

	flag.BoolVar(&withSlaveSync, "with-slave-sync", false, "sync to ardb slave")
	flag.BoolVar(&verbose, "v", false, "log verbose (debug) statements")

	// parse flags
	flag.Parse()

	// config logger (verbose or not)
	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Debugf("flags parsed: address=%q flush-size=%d flush-time=%d block-size=%d k=%d m=%d priv-key=%q nonce=%q profile-address=%q memorystorage=%t config=%q storage-addresses=%q",
		conf.ListenAddr,
		conf.FlushSize,
		conf.FlushTime,
		conf.BlockSize,
		conf.K,
		conf.M,
		conf.PrivKey,
		conf.HexNonce,
		profileAddr,
		inMemoryStorage,
		conf.ConfigPath,
		storageAddresses,
	)

	// profiling
	if profileAddr != "" {
		go func() {
			log.Infof("profiling enabled on %v", profileAddr)
			if err := http.ListenAndServe(profileAddr, http.DefaultServeMux); err != nil {
				log.Infof("Failed to enable profiling on %v, err:%v", profileAddr, err)
			}
		}()
	}

	// return server configs based on the given storage addresses
	serverConfigs, err := config.ParseCSStorageServerConfigStrings(storageAddresses)
	if err != nil {
		log.Fatal(err)
	}

	// create any kind of valid pool factory
	poolFactory, err := tlog.AnyRedisPoolFactory(tlog.RedisPoolFactoryConfig{
		RequiredDataServerCount: conf.RequiredDataServers(),
		ConfigPath:              conf.ConfigPath,
		ServerConfigs:           serverConfigs,
		AutoFill:                true,
		AllowInMemory:           true,
	})
	if err != nil {
		log.Fatalf("failed to create redis pool factory: %s", err.Error())
	}
	defer poolFactory.Close()

	if withSlaveSync {
		// aggregation MQ
		conf.AggMq = aggmq.NewMQ()

		// slave syncer manager
		ssm := slavesync.NewManager(conf.AggMq, conf.ConfigPath)
		go ssm.Run()
	}

	var fileConfig *config.Config
	if conf.ConfigPath != "" {
		fileConfig, err = config.ReadConfig(conf.ConfigPath, config.TlogServer)
		if err != nil {
			log.Fatalf("failed to read file config: %v", err)
		}
	}

	// create server
	server, err := server.NewServer(fileConfig, conf, poolFactory)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	server.Listen()
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
		fmt.Fprintln(os.Stderr, "Usage of", exe+":")
		flag.PrintDefaults()
	}
}
