package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestETCDKey(t *testing.T) {
	assert := assert.New(t)

	var validCases = []struct {
		Type     KeyType
		Expected string
	}{
		{KeyVdiskStatic, "foo:vdisk:conf:static"},
		{KeyVdiskNBD, "foo:vdisk:conf:storage:nbd"},
		{KeyVdiskTlog, "foo:vdisk:conf:storage:tlog"},
		{KeyClusterStorage, "foo:cluster:conf:storage"},
		{KeyClusterTlog, "foo:cluster:conf:tlog"},
		{KeyNBDServerVdisks, "foo:nbdserver:conf:vdisks"},
	}

	for _, validCase := range validCases {
		output, err := etcdKey("foo", validCase.Type)
		if assert.NoError(err, "input: %v", validCase.Type) {
			assert.Equal(validCase.Expected, output)
		}
	}
}
