package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/g8os/blockstor/redisstub"
	log "github.com/glendc/go-mini-log"
)

type config struct {
	K                 int
	M                 int
	flushSize         int
	flushTime         int
	privKey           string
	nonce             string
	objStoreAddresses []string
}

// ObjStorServerCount returns the amount of
// Object Store Servers are required/available/expected.
func (cfg *config) ObjStorServerCount() int {
	return cfg.K + cfg.M + 1
}

func (cfg *config) ObjStoreServerAddress() ([]string, error) {
	if expected := cfg.ObjStorServerCount(); len(cfg.objStoreAddresses) != expected {
		return nil, fmt.Errorf(
			"expected %d objstor servers to be available, while %d servers are available",
			expected, len(cfg.objStoreAddresses))
	}

	return cfg.objStoreAddresses, nil
}

func main() {
	var port int

	var conf config

	var verbose bool
	var objstoraddresses string

	flag.IntVar(&port, "port", 11211, "port to listen")
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
	flags := log.LstdFlags | log.Lshortfile
	if verbose {
		flags |= log.LDebug
	}
	log.SetFlags(flags)

	log.Debugf("port=%v\n", port)
	log.Debugf("k=%v, m=%v\n", conf.K, conf.M)

	if objstoraddresses != "" {
		conf.objStoreAddresses = strings.Split(objstoraddresses, ",")
	}
	length := len(conf.objStoreAddresses)
	expectedLength := conf.ObjStorServerCount()

	if length == 0 {
		// create in-memory ledisdb servers
		// if no object store address is given
		conf.objStoreAddresses = make([]string, expectedLength)
		for i := range conf.objStoreAddresses {
			// create in-memory redis
			objstor := redisstub.NewMemoryRedis()
			conf.objStoreAddresses[i] = objstor.Address()

			// listen to in-memory redis on new goroutine
			defer objstor.Close()
			go objstor.Listen()
		}
	} else if length < expectedLength {
		// parse last given address's host and port
		lastAddr := conf.objStoreAddresses[length-1]
		host, rawPort, err := net.SplitHostPort(lastAddr)
		if err != nil {
			log.Fatalf("objstor address %s is illegal: %v", lastAddr, err)
		}
		port, err := strconv.Atoi(rawPort)
		if err != nil {
			log.Fatalf("objstor address %s is illegal: %v", lastAddr, err)
		}
		// add the missing servers dynamically
		// (it is assumed that the missing servers are live on the ports
		//  following the last server's port)
		port++
		portBound := port + (expectedLength - length)
		for ; port < portBound; port++ {
			addr := net.JoinHostPort(host, strconv.Itoa(port))
			log.Debug("add missing objstor server address:", addr)
			conf.objStoreAddresses = append(conf.objStoreAddresses, addr)
		}
	} else if length > expectedLength {
		log.Fatalf(
			"too many objstor servers given, only %d are required",
			expectedLength)
	}

	// create server
	server, err := NewServer(port, &conf)
	if err != nil {
		log.Fatalf("failed to create server:%v\n", err)
	}

	server.Listen()
}
