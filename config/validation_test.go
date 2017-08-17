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
		"http://etcdserver.com:123",
		"https://etcdserver.com:123",
		"http://127.0.0.1:123",
		"https://127.0.0.1:123",
		"http://etcdserver.com:123/somepath",
		"https://etcdserver.com:123/somepath",
		"http://etcdserver.com:123/somepath#someid",
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
		"http://,foo.com",
		"https://foo.com;8080",
		"https://foo.com:8080\\somepath",
		"http://foo&bar.org",
		"http://foo bar.org",
	}
	for _, invalidCase := range invalidCases {
		assert.Falsef(IsServiceAddress(invalidCase), "%v", invalidCase)
	}
}
