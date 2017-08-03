package config

import (
	"testing"

	valid "github.com/asaskevich/govalidator"
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
		cfg, err := NewStorageClusterConfig([]byte(invalidCase))
		if assert.Error(err, invalidCase) {
			t.Logf("NewStorageClusterConfig error: %v", err)
			assert.Nil(cfg, invalidCase)
		}
	}
}

func TestStorageClusterConfigEqual(t *testing.T) {
	assert := assert.New(t)

	var a, b *StorageClusterConfig
	assert.True(a.Equal(b), "both are nil")

	a = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	}
	assert.False(a.Equal(b), "a isn't nil")
	b = a
	assert.True(a.Equal(b), "should be equal")

	a = nil
	assert.False(a.Equal(b), "a is nil")
	a = &StorageClusterConfig{
		DataStorage: []StorageServerConfig{
			StorageServerConfig{Address: "localhost:16379"},
		},
	}
	assert.True(a.Equal(b), "should be equal")

	b.DataStorage = append(b.DataStorage, StorageServerConfig{Address: "localhost:16380"})
	assert.False(a.Equal(b), "b has more servers")
	a.DataStorage = append(a.DataStorage, StorageServerConfig{Address: "localhost:16360"})
	assert.False(a.Equal(b), "equal amount of servers, but different")
	a.DataStorage[1].Address = "localhost:16380"
	assert.True(a.Equal(b), "equal servers")
	server := a.DataStorage[1]
	a.DataStorage[1] = a.DataStorage[0]
	a.DataStorage[0] = server
	assert.False(a.Equal(b), "equal servers, but different order")
	copy(a.DataStorage, b.DataStorage)
	assert.True(a.Equal(b), "equal servers")
	a.DataStorage[0].Database = 5
	assert.False(a.Equal(b), "almost equal servers, one has different database")
	b.DataStorage[0].Database = 5
	assert.True(a.Equal(b), "equal servers")

	a.MetadataStorage = &StorageServerConfig{Address: "localhost:16379"}
	assert.False(a.Equal(b), "difference because of metadata server")
	b.MetadataStorage = &StorageServerConfig{Address: "localhost:16380"}
	assert.False(a.Equal(b), "difference because of metadata server")
	b.MetadataStorage.Address = "localhost:16379"
	assert.True(a.Equal(b), "equal servers")
	b.MetadataStorage.Database = 2
	assert.False(a.Equal(b), "difference because of metadata server")
	a.MetadataStorage.Database = 2
	assert.True(a.Equal(b), "equal servers")
	a.MetadataStorage = nil
	assert.False(a.Equal(b), "difference because of metadata server")

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
	assert.Empty(a.DataStorage)
	assert.Nil(a.MetadataStorage)

	a.DataStorage = []StorageServerConfig{
		StorageServerConfig{Address: "localhost:16379"},
		StorageServerConfig{Address: "localhost:16380"},
		StorageServerConfig{Address: "localhost:16381"},
	}
	a.MetadataStorage = &StorageServerConfig{Address: "localhost:16379"}

	b := a.Clone()
	assert.True(a.Equal(&b), "should be equal")

	// as b is a clone, we should be able to modify it, without modifying a
	b.DataStorage[0].Address = "localhost:300"
	assert.False(a.Equal(&b), "shouldn't be equal")
	a.DataStorage[0].Address = "localhost:300"
	assert.True(a.Equal(&b), "should be equal")

	// this also applies to the metadata storage
	a.MetadataStorage.Database = 42
	assert.False(a.Equal(&b), "shouldn't be equal")
	b.MetadataStorage.Database = 42
	assert.True(a.Equal(&b), "should be equal")
}

// tests StorageServerConfig manual unmarshalling
func TestStorageServerConfigYamlUnmarshal(t *testing.T) {
	assert := assert.New(t)

	for _, validCase := range validStorageServerConfigYAML {
		var cfg StorageServerConfig
		err := yaml.Unmarshal([]byte(validCase), &cfg)
		if assert.NoErrorf(err, "'%v'", validCase) {
			_, err = valid.ValidateStruct(&cfg)
			assert.NoErrorf(err, "'%v'", validCase)
		}
	}

	for _, invalidCase := range invalidStorageServerConfigYAML {
		var cfg StorageServerConfig
		err := yaml.Unmarshal([]byte(invalidCase), &cfg)
		if err == nil {
			_, err = valid.ValidateStruct(&cfg)
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
