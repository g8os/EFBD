package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog/tlogserver/server"
)

func main() {
	conf := server.DefaultConfig()

	var verbose bool
	var profileAddr string
	var objstoraddresses string

	flag.StringVar(&conf.ListenAddr, "address", conf.ListenAddr, "Address to listen on")
	flag.IntVar(&conf.FlushSize, "flush-size", conf.FlushSize, "flush size")
	flag.IntVar(&conf.FlushTime, "flush-time", conf.FlushTime, "flush time (seconds)")
	flag.IntVar(&conf.BlockSize, "block-size", conf.BlockSize, "block size (bytes)")
	flag.IntVar(&conf.K, "k", conf.K, "K variable of the erasure encoding")
	flag.IntVar(&conf.M, "m", conf.M, "M variable of the erasure encoding")
	flag.StringVar(&conf.PrivKey, "priv-key", conf.PrivKey, "private key")
	flag.StringVar(&conf.HexNonce, "nonce", conf.HexNonce, "hex nonce used for encryption")
	flag.StringVar(&profileAddr, "profile-address", "", "Enables profiling of this server as an http service")

	flag.StringVar(&objstoraddresses, "storage-addresses", "",
		"comma seperated list of redis compatible connectionstrings, if < k+m+1 addresses are given, the missing addresses are assumed to be on the ports following the last given address")

	flag.BoolVar(&verbose, "v", false, "log verbose (debug) statements")

	// parse flags
	flag.Parse()

	// profiling
	if profileAddr != "" {
		go func() {
			log.Infof("profiling enabled on %v", profileAddr)
			if err := http.ListenAndServe(profileAddr, http.DefaultServeMux); err != nil {
				log.Infof("Failed to enable profiling on %v, err:%v", profileAddr, err)
			}
		}()
	}

	// get objstore addresses
	if objstoraddresses != "" {
		conf.ObjStoreAddresses = strings.Split(objstoraddresses, ",")
	}

	// config logger (verbose or not)
	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	const allowStubs = true
	err := conf.ValidateAndCreateObjStoreAddresses(allowStubs)
	if err != nil {
		log.Fatalf("failed to create config: %v", err)
	}

	log.Debugf("listen addr=%v\n", conf.ListenAddr)
	log.Debugf("k=%v, m=%v\n", conf.K, conf.M)

	// create server
	server, err := server.NewServer(conf)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	server.Listen()
}
