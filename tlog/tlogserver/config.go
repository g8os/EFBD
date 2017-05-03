package main

import (
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
	listenAddr        string
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

func (cfg *config) initObjStoreAddress(objstoraddresses string) {
	if objstoraddresses != "" {
		cfg.objStoreAddresses = strings.Split(objstoraddresses, ",")
	}
	length := len(cfg.objStoreAddresses)
	expectedLength := cfg.ObjStorServerCount()

	if length == 0 {
		// create in-memory ledisdb servers
		// if no object store address is given
		cfg.objStoreAddresses = make([]string, expectedLength)
		for i := range cfg.objStoreAddresses {
			// create in-memory redis
			objstor := redisstub.NewMemoryRedis()
			cfg.objStoreAddresses[i] = objstor.Address()

			// listen to in-memory redis on new goroutine
			//defer objstor.Close()
			go objstor.Listen()
		}
	} else if length < expectedLength {
		// parse last given address's host and port
		lastAddr := cfg.objStoreAddresses[length-1]
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
			cfg.objStoreAddresses = append(cfg.objStoreAddresses, addr)
		}
	} else if length > expectedLength {
		log.Fatalf(
			"too many objstor servers given, only %d are required",
			expectedLength)
	}

}
