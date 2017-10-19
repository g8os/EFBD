package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v2"
)

// tests NBDVdisksConfig bytes constructor and Validate function.
func TestNewNBDVdisksConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validNBDVdisksConfigYAML {
		cfg, err := NewNBDVdisksConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			if assert.NotNil(cfg, validCase) {
				assert.NotEmpty(cfg.Vdisks, validCase)
			}
		}
	}

	for _, invalidCase := range invalidNBDVdisksConfigYAML {
		cfg, err := NewNBDVdisksConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewNBDVdisksConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests VdiskStaticConfig bytes constructor and Validate function.
func TestNewVdiskStaticConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVdiskStaticConfigYAML {
		cfg, err := NewVdiskStaticConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidVdiskStaticConfigYAML {
		cfg, err := NewVdiskStaticConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewVdiskStaticConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests VdiskNBDConfig bytes constructor and Validate function.
func TestNewVdiskNBDConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVdiskNBDConfigYAML {
		cfg, err := NewVdiskNBDConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidVdiskNBDConfigYAML {
		cfg, err := NewVdiskNBDConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewVdiskNBDConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests VdiskTlogConfig bytes constructor and Validate function.
func TestNewVdiskTlogConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validVdiskTlogConfigYAML {
		cfg, err := NewVdiskTlogConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidVdiskTlogConfigYAML {
		cfg, err := NewVdiskTlogConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewVdiskTlogConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

// tests StorageClusterConfig bytes constructor and Validate function.
func TestNewStorageClusterConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validStorageClusterConfigYAML {
		cfg, err := NewStorageClusterConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidStorageClusterConfigYAML {
		_, err := NewStorageClusterConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewStorageClusterConfig error: %v", err)
		}
	}
}

func TestStorageClusterConfigEqual(t *testing.T) {
	assert := assert.New(t)

	var a, b *StorageClusterConfig
	assert.True(a.Equal(b), "both are nil")

	a = &StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	}
	assert.False(a.Equal(b), "a isn't nil")
	b = a
	assert.True(a.Equal(b), "should be equal")

	a = nil
	assert.False(a.Equal(b), "a is nil")
	a = &StorageClusterConfig{
		Servers: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	}
	assert.True(a.Equal(b), "should be equal")

	b.Servers = append(b.Servers, StorageServerConfig{Address: "localhost:16380"})
	assert.False(a.Equal(b), "b has more servers")
	a.Servers = append(a.Servers, StorageServerConfig{Address: "localhost:16360"})
	assert.False(a.Equal(b), "equal amount of servers, but different")
	a.Servers[1].Address = "localhost:16380"
	assert.True(a.Equal(b), "equal servers")
	server := a.Servers[1]
	a.Servers[1] = a.Servers[0]
	a.Servers[0] = server
	assert.False(a.Equal(b), "equal servers, but different order")
	copy(a.Servers, b.Servers)
	assert.True(a.Equal(b), "equal servers")
	a.Servers[0].Database = 5
	assert.False(a.Equal(b), "almost equal servers, one has different database")
	b.Servers[0].Database = 5

	b = nil
	assert.False(a.Equal(b), "b is nil")
}

func TestStorageServerConfigEqual(t *testing.T) {
	assert := assert.New(t)

	var a, b *StorageServerConfig
	assert.True(a.Equal(b), "both are nil")

	a = &StorageServerConfig{Address: "localhost:16379"}
	assert.False(a.Equal(b), "a isn't nil")

	b = a
	assert.True(a.Equal(b), "should be equal")

	a = nil
	assert.False(a.Equal(b), "a is nil")

	a = &StorageServerConfig{Address: "localhost:16379"}
	assert.True(a.Equal(b), "should be equal")

	a.Database = 42
	assert.False(a.Equal(b), "a has different database")
	b.Database = 42
	assert.True(a.Equal(b), "should be equal")

	a = nil
	assert.False(a.Equal(b), "a is nil")
	b = nil
	assert.True(a.Equal(b), "both are nil")
}

func TestStorageClusterConfigClone(t *testing.T) {
	assert := assert.New(t)

	var nilCluster *StorageClusterConfig
	// should be fine, will be just a nil cluster
	a := nilCluster.Clone()
	assert.Empty(a.Servers)

	a.Servers = []StorageServerConfig{
		StorageServerConfig{Address: "localhost:16379"},
		StorageServerConfig{Address: "localhost:16380"},
		StorageServerConfig{Address: "localhost:16381"},
	}

	b := a.Clone()
	assert.True(a.Equal(&b), "should be equal")

	// as b is a clone, we should be able to modify it, without modifying a
	b.Servers[0].Address = "localhost:300"
	assert.False(a.Equal(&b), "shouldn't be equal")
	a.Servers[0].Address = "localhost:300"
	assert.True(a.Equal(&b), "should be equal")
}

// tests StorageServerConfig manual unmarshalling
func TestStorageServerConfigYamlUnmarshalAndValidate(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validStorageServerConfigYAML {
		var cfg StorageServerConfig
		err := yaml.Unmarshal([]byte(validCase), &cfg)
		if assert.NoErrorf(err, "'%v'", validCase) {
			err = cfg.Validate()
			assert.NoErrorf(err, "'%v'", validCase)
		}
	}

	for _, invalidCase := range invalidStorageServerConfigYAML {
		var cfg StorageServerConfig
		err := yaml.Unmarshal([]byte(invalidCase), &cfg)
		if err == nil {
			err = cfg.Validate()
			if assert.Errorf(err, "'%v' -> %v'", invalidCase, cfg) {
				t.Logf("StorageServerConfig error: %v", err)
			}
		} else {
			t.Logf("StorageServerConfig error: %v", err)
		}
	}
}

func TestTlogClusterConfigClone(t *testing.T) {
	assert := assert.New(t)

	var nilCluster *TlogClusterConfig
	// should be fine, will be just a nil cluster
	a := nilCluster.Clone()
	assert.Empty(a.Servers)

	a.Servers = []string{
		"localhost:16379",
		"localhost:16380",
		"localhost:16381",
	}

	b := a.Clone()
	assert.Equal(a.Servers, b.Servers, "should be equal")

	b.Servers[0] = "localhost:200"
	assert.NotEqual(a.Servers, b.Servers, "one server isn't equal any longer")
	a.Servers[0] = "localhost:200"
	assert.Equal(a.Servers, b.Servers, "should be equal")
}

func TestNewZeroStorClusterConfig(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validZeroStorClusterConfigYAML {
		cfg, err := NewZeroStorClusterConfig([]byte(validCase))
		if assert.NoError(err, validCase) {
			assert.NotNil(cfg, validCase)
		}
	}

	for _, invalidCase := range invalidZeroStorClusterConfigYAML {
		cfg, err := NewZeroStorClusterConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewStorageClusterConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}
func TestZeroStorClusterConfigEqual(t *testing.T) {
	assert := assert.New(t)

	var a, b *ZeroStorClusterConfig
	assert.True(a.Equal(b), "both are nil")

	a = &ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org: "foo organisation",
		},
	}
	assert.False(a.Equal(b), "a is not nil")
	b = a
	assert.True(a.Equal(b), "should be equal")

	a = nil
	assert.False(a.Equal(b), "a is nil")

	a = &ZeroStorClusterConfig{
		IYO: IYOCredentials{
			Org:       "foo organisation",
			Namespace: "foo namespace",
		},
	}
	assert.False(a.Equal(b), "b does not have a namespace")
	b.IYO.Namespace = "bar namespace"
	assert.False(a.Equal(b), "b has a different namespace")
	b.IYO.Namespace = "foo namespace"
	assert.True(a.Equal(b), "should be equal")
	a.IYO.ClientID = "foo client"
	assert.False(a.Equal(b), "b does not have a clientID")
	b.IYO.ClientID = "bar client"
	assert.False(a.Equal(b), "b has a different clientID")
	b.IYO.ClientID = "foo client"
	assert.True(a.Equal(b), "should be equal")
	a.IYO.Secret = "foo secret"
	assert.False(a.Equal(b), "b does not have a secrect")
	b.IYO.Secret = "bar secret"
	assert.False(a.Equal(b), "b has a different secret")
	b.IYO.Secret = "foo secret"
	assert.True(a.Equal(b), "should be equal")

	a.Servers = []ServerConfig{
		ServerConfig{
			Address: "1.1.1.1:11",
		},
	}
	assert.False(a.Equal(b), "b does not have a server")
	b.Servers = []ServerConfig{
		ServerConfig{
			Address: "1.1.1.1:22",
		},
	}
	assert.False(a.Equal(b), "b has a different server address")
	b.Servers[0].Address = "1.1.1.1:11"
	assert.True(a.Equal(b), "should be equal")

	a.MetadataServers = []ServerConfig{
		ServerConfig{
			Address: "1.1.1.1:11",
		},
	}
	assert.False(a.Equal(b), "b does not have a metadata server")
	b.MetadataServers = []ServerConfig{
		ServerConfig{
			Address: "1.1.1.1:22",
		},
	}
	assert.False(a.Equal(b), "b has a different server metadata address")
	b.MetadataServers[0].Address = "1.1.1.1:11"
	assert.True(a.Equal(b), "should be equal")
}

func TestZeroStorClusterConfigClone(t *testing.T) {
	assert := assert.New(t)

	var nilCluster *ZeroStorClusterConfig
	// should be fine, will be just a nil cluster
	a := nilCluster.Clone()
	assert.Empty(a.IYO.ClientID)
	assert.Empty(a.IYO.Namespace)
	assert.Empty(a.IYO.Org)
	assert.Empty(a.IYO.Secret)
	assert.Empty(a.Servers)
	assert.Empty(a.MetadataServers)

	a.IYO = IYOCredentials{
		Org:       "foo org",
		Namespace: "foo namespace",
		ClientID:  "client foo",
		Secret:    "secret foo",
	}

	a.Servers = []ServerConfig{
		ServerConfig{"localhost:16379"},
		ServerConfig{"localhost:16380"},
		ServerConfig{"localhost:16381"},
	}

	a.MetadataServers = []ServerConfig{
		ServerConfig{"localhost:16389"},
		ServerConfig{"localhost:16390"},
		ServerConfig{"localhost:16391"},
	}

	b := a.Clone()
	assert.Equal(a.IYO, b.IYO, "should be equal")
	assert.Equal(a.Servers, b.Servers, "should be equal")
	assert.Equal(a.MetadataServers, b.MetadataServers, "should be equal")

	b.IYO.Secret = "secret bar"
	b.Servers[0] = ServerConfig{"localhost:200"}
	b.MetadataServers[0] = ServerConfig{"localhost:201"}

	assert.NotEqual(a.IYO, b.IYO, "IYO secret shouldn't equal any longer")
	assert.NotEqual(a.Servers, b.Servers, "one server shouldn't equal any longer")
	assert.NotEqual(a.MetadataServers, b.MetadataServers, "one server shouldn't equal any longer")

}
