package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsServiceAddress(t *testing.T) {
	assert := assert.New(t)

	validCases := []string{
		"localhost:3000",
		"unix://foo",
		"unix://42",
		"127.0.0.1:3000",
		"[2001:db8:0:1:1:1:1:1]:123",
	}
	for _, validCase := range validCases {
		assert.Truef(IsServiceAddress(validCase), "%v", validCase)
	}

	invalidCases := []string{
		"localhost",
		"127.0.0.1",
		"localhost:foo",
		"unix:/foo",
		"unix:boo",
	}
	for _, invalidCase := range invalidCases {
		assert.Falsef(IsServiceAddress(invalidCase), "%v", invalidCase)
	}
}
