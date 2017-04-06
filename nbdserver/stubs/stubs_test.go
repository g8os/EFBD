package stubs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionStringToStorageServer(t *testing.T) {
	ss, _ := connectionStringToStorageServer("123.123.123.123:6666")
	assert.Equal(t, "123.123.123.123", ss.Ip)
	assert.Equal(t, 6666, ss.Port)
}
