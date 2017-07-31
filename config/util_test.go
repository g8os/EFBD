package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseValidCSStorageServerConfigStrings(t *testing.T) {
	testCases := []struct {
		input    string
		expected []StorageServerConfig
	}{
		{"", nil},
		{",", nil},
		{",,,,", nil},
		{"0.0.0.0:1", scconfigs("0.0.0.0:1", 0)},
		{"0.0.0.0:1 ", scconfigs("0.0.0.0:1", 0)},
		{" 0.0.0.0:1", scconfigs("0.0.0.0:1", 0)},
		{" 0.0.0.0:1 ", scconfigs("0.0.0.0:1", 0)},
		{"0.0.0.0:1@0", scconfigs("0.0.0.0:1", 0)},
		{"0.0.0.0:1@1", scconfigs("0.0.0.0:1", 1)},
		{"0.0.0.0:1@1,", scconfigs("0.0.0.0:1", 1)},
		{"0.0.0.0:1@1,,,,", scconfigs("0.0.0.0:1", 1)},
		{"0.0.0.0:1,0.0.0.0:1", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1,0.0.0.0:1,", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1, 0.0.0.0:1", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1, 0.0.0.0:1 ", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 0)},
		{"0.0.0.0:1,0.0.0.0:1@1", scconfigs("0.0.0.0:1", 0, "0.0.0.0:1", 1)},
		{"1.2.3.4:5@6,7.8.9.10:11@12", scconfigs("1.2.3.4:5", 6, "7.8.9.10:11", 12)},
	}

	for _, testCase := range testCases {
		serverConfigs, err := ParseCSStorageServerConfigStrings(testCase.input)
		if assert.Nil(t, err, testCase.input) {
			assert.Equal(t, testCase.expected, serverConfigs)
		}
	}
}

// scconfigs allows for quickly generating server configs,
// for testing purposes
func scconfigs(argv ...interface{}) (serverConfigs []StorageServerConfig) {
	argn := len(argv)
	for i := 0; i < argn; i += 2 {
		serverConfigs = append(serverConfigs, StorageServerConfig{
			Address:  argv[i].(string),
			Database: argv[i+1].(int),
		})
	}
	return
}
