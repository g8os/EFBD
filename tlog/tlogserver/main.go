package main

import (
	"flag"
	"strings"

	"github.com/g8os/blockstor/tlog/tlogserver/server"
	log "github.com/glendc/go-mini-log"
)

func main() {
	conf := server.DefaultConfig()

	var verbose bool
	var objstoraddresses string

	flag.StringVar(&conf.ListenAddr, "listen-addr", conf.ListenAddr, "port to listen")
	flag.IntVar(&conf.FlushSize, "flush-size", conf.FlushSize, "flush size")
	flag.IntVar(&conf.FlushTime, "flush-time", conf.FlushTime, "flush time (seconds)")
	flag.IntVar(&conf.K, "k", conf.K, "K variable of erasure encoding")
	flag.IntVar(&conf.M, "m", conf.M, "M variable of erasure encoding")
	flag.StringVar(&conf.PrivKey, "priv-key", conf.PrivKey, "private key")
	flag.StringVar(&conf.HexNonce, "nonce", conf.HexNonce, "hex nonce used for encryption")

	flag.StringVar(&objstoraddresses, "objstor-addresses", "",
		"comma seperated list of objstor addresses, if < k+m+1 addresses are given, the missing addresses are assumed to be on the ports following the last given address")

	flag.BoolVar(&verbose, "v", false, "log verbose (debug) statements")

	// parse flags
	flag.Parse()

	// get objstore addresses
	if objstoraddresses != "" {
		conf.ObjStoreAddresses = strings.Split(objstoraddresses, ",")
	}

	// config logger (verbose or not)
	flags := log.LstdFlags | log.Lshortfile
	if verbose {
		flags |= log.LDebug
	}
	log.SetFlags(flags)

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
