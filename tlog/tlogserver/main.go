package main

import (
	"flag"

	"github.com/g8os/blockstor/log"
)

func main() {
	var conf config

	var verbose bool
	var objstoraddresses string

	flag.StringVar(&conf.listenAddr, "listen-addr", "0.0.0.0:11211", "port to listen")
	flag.IntVar(&conf.flushSize, "flush-size", 25, "flush size")
	flag.IntVar(&conf.flushTime, "flush-time", 25, "flush time (seconds)")
	flag.IntVar(&conf.K, "k", 4, "K variable of erasure encoding")
	flag.IntVar(&conf.M, "m", 2, "M variable of erasure encoding")
	flag.StringVar(&objstoraddresses, "objstor-addresses", "",
		"comma seperated list of objstor addresses, if < k+m+1 addresses are given, the missing addresses are assumed to be on the ports following the last given address")
	flag.StringVar(&conf.privKey, "priv-key", "12345678901234567890123456789012", "private key")
	flag.StringVar(&conf.nonce, "nonce", "37b8e8a308c354048d245f6d", "hex nonce used for encryption")
	flag.BoolVar(&verbose, "v", false, "log verbose (debug) statements")

	flag.Parse()

	// config logger (verbose or not)
	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.Debugf("listen addr=%v\n", conf.listenAddr)
	log.Debugf("k=%v, m=%v\n", conf.K, conf.M)

	conf.initObjStoreAddress(objstoraddresses)

	// create server
	server, err := NewServer(&conf)
	if err != nil {
		log.Fatalf("failed to create server:%v\n", err)
	}

	server.Listen()
}
