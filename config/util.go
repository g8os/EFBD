package config

import (
	"fmt"
	"strconv"
	"strings"

	valid "github.com/asaskevich/govalidator"
)

// ParseStorageServerConfigString allows you to parse a raw dial config string.
// Dial Config String are a simple format used to specify ardb connection configs
// easily as a command line argument.
// The format is as follows: `<ip>:<port>[@<db_index>]`,
// where the db_index is optional.
// The parsing algorithm of this function is very forgiving,
// and returns an error only in case an invalid address is given.
func ParseStorageServerConfigString(dialConfigString string) (config StorageServerConfig, err error) {
	parts := strings.Split(dialConfigString, "@")
	if n := len(parts); n < 2 {
		config.Address = dialConfigString
	} else {
		config.Database, err = strconv.Atoi(parts[n-1])
		if err != nil {
			err = nil // ignore actual error
			n++       // not a valid database, thus probably part of address
		}
		// join any other parts back together,
		// if for some reason an @ sign makes part of the address
		config.Address = strings.Join(parts[:n-1], "@")
	}

	if !valid.IsDialString(config.Address) {
		err = fmt.Errorf("%s is not a valid storage address", config.Address)
	}

	return
}

// ParseCSStorageServerConfigStrings allows you to parse a slice of raw dial config strings.
// Dial Config Strings are a simple format used to specify ardb connection configs
// easily as a command line argument.
// The format is as follows: `<ip>:<port>[@<db_index>][,<ip>:<port>[@<db_index>]]`,
// where the db_index is optional, and you can give multiple configs by
// seperating them with a comma.
// The parsing algorithm of this function is very forgiving,
// and returns an error only in case an invalid address is given.
func ParseCSStorageServerConfigStrings(dialCSConfigString string) (configs []StorageServerConfig, err error) {
	if dialCSConfigString == "" {
		return nil, nil
	}
	dialConfigStrings := strings.Split(dialCSConfigString, ",")

	var cfg StorageServerConfig

	// convert all connection strings into ConnectionConfigs
	for _, dialConfigString := range dialConfigStrings {
		// remove whitespace around
		dialConfigString = strings.TrimSpace(dialConfigString)

		// trailing commas are allowed
		if dialConfigString == "" {
			continue
		}

		// parse one storage server config string
		cfg, err = ParseStorageServerConfigString(dialConfigString)
		if err != nil {
			configs = nil
			return
		}

		configs = append(configs, cfg)
	}

	return
}
