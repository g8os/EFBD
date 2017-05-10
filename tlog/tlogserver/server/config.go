package server

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/g8os/blockstor/redisstub"
	log "github.com/glendc/go-mini-log"
)

// DefaultConfig creates a new config, using sane defaults
func DefaultConfig() *Config {
	return &Config{
		K:          4,
		M:          2,
		ListenAddr: "0.0.0.0:11211",
		FlushSize:  25,
		FlushTime:  25,
		BlockSize:  4096,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}
}

// Config used for creating the tlogserver
type Config struct {
	K                 int
	M                 int
	BlockSize         int // size of each block, used as hint for the flusher buffer size
	ListenAddr        string
	FlushSize         int
	FlushTime         int
	PrivKey           string
	HexNonce          string
	ObjStoreAddresses []string
}

// ValidateAndCreateObjStoreAddresses checks if the config
// contains exactly as many object store server addresses as expected.
// If more are given, an error is returned.
// If none are given, memorystubs can be created if `allowInMemoryStubs` is true, otherwise an error is returned.
// If less are given, the missing addresses are generated, using consecutive ports,
// starting with the port following the port of the last given object store server address
func (cfg *Config) ValidateAndCreateObjStoreAddresses(allowInMemoryStubs bool) error {
	length := len(cfg.ObjStoreAddresses)
	expectedLength := cfg.ObjStorServerCount()

	if length == 0 {
		if !allowInMemoryStubs {
			return errors.New("no objstore servers available, and inmemory stubs aren't allowed")
		}

		// create in-memory ledisdb servers
		// if no object store address is given
		cfg.ObjStoreAddresses = make([]string, expectedLength)
		for i := range cfg.ObjStoreAddresses {
			// create in-memory redis
			objstor := redisstub.NewMemoryRedis()
			cfg.ObjStoreAddresses[i] = objstor.Address()

			// listen to in-memory redis on new goroutine
			//defer objstor.Close()
			go objstor.Listen()
		}
	} else if length < expectedLength {
		// parse last given address's host and port
		lastAddr := cfg.ObjStoreAddresses[length-1]
		host, rawPort, err := net.SplitHostPort(lastAddr)
		if err != nil {
			return fmt.Errorf("objstor address %s is illegal: %v", lastAddr, err)
		}
		port, err := strconv.Atoi(rawPort)
		if err != nil {
			return fmt.Errorf("objstor address %s is illegal: %v", lastAddr, err)
		}
		// add the missing servers dynamically
		// (it is assumed that the missing servers are live on the ports
		//  following the last server's port)
		port++
		portBound := port + (expectedLength - length)
		for ; port < portBound; port++ {
			addr := net.JoinHostPort(host, strconv.Itoa(port))
			log.Debug("add missing objstor server address:", addr)
			cfg.ObjStoreAddresses = append(cfg.ObjStoreAddresses, addr)
		}
	} else if length > expectedLength {
		return fmt.Errorf(
			"too many objstor servers given, only %d are required",
			expectedLength)
	}

	return nil
}

// ObjStorServerCount returns the amount of
// Object Store Servers are required/available/expected.
func (cfg *Config) ObjStorServerCount() int {
	return cfg.K + cfg.M + 1
}

// ObjStoreServerAddresses returns the objstore server addresses,
// iff the config defines exactly as many of the expected server addresses
func (cfg *Config) ObjStoreServerAddresses() ([]string, error) {
	if expected := cfg.ObjStorServerCount(); len(cfg.ObjStoreAddresses) != expected {
		return nil, fmt.Errorf(
			"expected %d objstor servers to be available, while %d servers are available",
			expected, len(cfg.ObjStoreAddresses))
	}

	return cfg.ObjStoreAddresses, nil
}
