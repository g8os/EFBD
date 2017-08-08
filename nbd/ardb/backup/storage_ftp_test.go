package backup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinimalFTPStorageConfigToString(t *testing.T) {
	assert := assert.New(t)

	validCases := []struct {
		Input, Output string
	}{
		{"foo", "ftp://foo:21"},
		{"foo/bar/baz", "ftp://foo:21/bar/baz"},
		{"foo:22", "ftp://foo:22"},
	}

	for _, validCase := range validCases {
		var cfg FTPStorageConfig
		if !assert.NoError(cfg.Set(validCase.Input)) {
			continue
		}
		assert.Equal(validCase.Output, cfg.String())
	}
}

func TestFTPStorageConfigToString(t *testing.T) {
	assert := assert.New(t)

	validCases := []struct {
		Config   FTPStorageConfig
		Expected string
	}{
		{FTPStorageConfig{Address: "localhost:2000"}, "ftp://localhost:2000"},
		{FTPStorageConfig{Address: "localhost:2000/bar/foo"}, "ftp://localhost:2000/bar/foo"},
		{FTPStorageConfig{Address: "localhost:2000/bar"}, "ftp://localhost:2000/bar"},
		{FTPStorageConfig{Address: "localhost:2000", Username: "foo"}, "ftp://foo@localhost:2000"},
		{FTPStorageConfig{Address: "localhost:2000", Username: "foo", Password: "boo"}, "ftp://foo:boo@localhost:2000"},
		{FTPStorageConfig{Address: "localhost:2000/bar", Username: "foo", Password: "boo"}, "ftp://foo:boo@localhost:2000/bar"},
	}

	for _, validCase := range validCases {
		output := validCase.Config.String()
		assert.Equal(validCase.Expected, output)
	}
}

func TestFTPStorageConfigStringCommute(t *testing.T) {
	assert := assert.New(t)

	validCases := []string{
		"localhost:2000",
		"localhost:2000",
		"localhost:2000/foo",
		"ftp://localhost:2000",
		"ftp://localhost:2000/foo",
		"username@localhost:2000",
		"username@localhost:200/foo0",
		"ftp://username@localhost:2000/bar/foo",
		"user:pass@localhost:3000",
		"user:pass@localhost:3000/bar",
		"ftp://user:pass@localhost:3000/bar",
	}

	for _, validCase := range validCases {
		var cfg FTPStorageConfig
		if !assert.NoError(cfg.Set(validCase)) {
			continue
		}

		expected := validCase
		if !strings.HasPrefix(expected, "ftp://") {
			expected = "ftp://" + expected
		}
		assert.Equal(expected, cfg.String())
	}
}
