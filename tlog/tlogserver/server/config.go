package server

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/redisstub"
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
	K                int
	M                int
	BlockSize        int // size of each block, used as hint for the flusher buffer size
	ListenAddr       string
	FlushSize        int
	FlushTime        int
	PrivKey          string
	HexNonce         string
	StorageAddresses []string
}

// ValidateAndCreateStorageAddresses checks if the config
// contains exactly as many storage server addresses as expected.
// If more are given, an error is returned.
// If none are given, memorystubs can be created if `allowInMemoryStubs` is true, otherwise an error is returned.
// If less are given, the missing addresses are generated, using consecutive ports,
// starting with the port following the port of the last given storage server address
func (cfg *Config) ValidateAndCreateStorageAddresses(allowInMemoryStubs bool) error {
	length := len(cfg.StorageAddresses)
	expectedLength := cfg.StorageServerCount()

	if length == 0 {
		if !allowInMemoryStubs {
			return errors.New("no storage servers available, and inmemory stubs aren't allowed")
		}

		// create in-memory ledisdb servers
		// if no object store address is given
		cfg.StorageAddresses = make([]string, expectedLength)
		for i := range cfg.StorageAddresses {
			// create in-memory redis
			stor := redisstub.NewMemoryRedis()
			cfg.StorageAddresses[i] = stor.Address()

			// listen to in-memory redis on new goroutine
			//defer stor.Close()
			go stor.Listen()
		}
	} else if length < expectedLength {
		// parse last given address's host and port
		lastAddr := cfg.StorageAddresses[length-1]
		host, rawPort, err := net.SplitHostPort(lastAddr)
		if err != nil {
			return fmt.Errorf("storage address %s is illegal: %v", lastAddr, err)
		}
		port, err := strconv.Atoi(rawPort)
		if err != nil {
			return fmt.Errorf("storage address %s is illegal: %v", lastAddr, err)
		}
		// add the missing servers dynamically
		// (it is assumed that the missing servers are live on the ports
		//  following the last server's port)
		port++
		portBound := port + (expectedLength - length)
		for ; port < portBound; port++ {
			addr := net.JoinHostPort(host, strconv.Itoa(port))
			log.Debug("add missing objstor server address:", addr)
			cfg.StorageAddresses = append(cfg.StorageAddresses, addr)
		}
	} else if length > expectedLength {
		return fmt.Errorf(
			"too many storage servers given, only %d are required",
			expectedLength)
	}

	return nil
}

// StorageServerCount returns the amount of
// Storage Servers are required/available/expected.
func (cfg *Config) StorageServerCount() int {
	return cfg.K + cfg.M + 1
}

// StorageServerAddresses returns the storage server addresses,
// if the config defines exactly as many of the expected server addresses
func (cfg *Config) StorageServerAddresses() ([]string, error) {
	if expected := cfg.StorageServerCount(); len(cfg.StorageAddresses) != expected {
		return nil, fmt.Errorf(
			"expected %d objstor servers to be available, while %d servers are available",
			expected, len(cfg.StorageAddresses))
	}

	return cfg.StorageAddresses, nil
}

// ConnectionConfig is given by the client to the server,
// during the handshake phase.
type ConnectionConfig struct {
	VdiskID       string
	FirstSequence uint64
}
