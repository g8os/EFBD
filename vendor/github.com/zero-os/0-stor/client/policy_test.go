package client

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	f, err := os.Open("./fixtures/config.yaml")
	assert.Nil(t, err)

	policy, err := NewPolicyFromReader(f)
	assert.Nil(t, err)

	except := Policy{
		Organization: "mordor",
		Namespace:    "thedisk",
		IYOAppID:     "1234",
		IYOSecret:    "45678",
		DataShards: []string{
			"http://127.0.0.1:12345",
			"http://127.0.0.1:12346",
			"http://127.0.0.1:12347",
			"http://127.0.0.1:12348",
		},
		MetaShards: []string{
			"http://127.0.0.1:23790",
		},
		BlockSize:              1048576,
		ReplicationNr:          4,
		ReplicationMaxSize:     4194304,
		DistributionNr:         3,
		DistributionRedundancy: 1,
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             "ab345678901234567890123456789012",
	}

	assert.Equal(t, except, policy)
}
