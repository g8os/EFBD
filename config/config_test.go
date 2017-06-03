package config

import (
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
)

const validConfig = `
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
        db: 0
      - address: 192.123.123.123:2001
        db: 0
    metadataStorage:
      address: 192.168.58.146:2001
      db: 1
  rootcluster:
    dataStorage:
      - address: 192.168.58.147:2000
        db: 0
    metadataStorage:
      address: 192.168.58.147:2001
      db: 2
  tlogcluster:
    dataStorage:
      - address: 192.168.58.149:2000
        db: 4
    metadataStorage:
      address: 192.168.58.149:2000
      db: 8
vdisks:
  myvdisk:
    blockSize: 4096
    readOnly: false
    size: 10
    storageCluster: mycluster
    rootStorageCluster: rootcluster
    tlogStorageCluster: tlogcluster
    type: boot`

func TestValidConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(validConfig))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 3) {
		if cluster, ok := cfg.StorageClusters["mycluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 2) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0].Address)
				assert.Equal(t, 0, cluster.DataStorage[0].Database)
				assert.Equal(t, "192.123.123.123:2001", cluster.DataStorage[1].Address)
				assert.Equal(t, 0, cluster.DataStorage[1].Database)
			}
			assert.Equal(t, "192.168.58.146:2001", cluster.MetadataStorage.Address)
			assert.Equal(t, 1, cluster.MetadataStorage.Database)
		}

		if cluster, ok := cfg.StorageClusters["rootcluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.147:2000", cluster.DataStorage[0].Address)
				assert.Equal(t, 0, cluster.DataStorage[0].Database)
			}
			assert.Equal(t, "192.168.58.147:2001", cluster.MetadataStorage.Address)
			assert.Equal(t, 2, cluster.MetadataStorage.Database)
		}

		if cluster, ok := cfg.StorageClusters["tlogcluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.149:2000", cluster.DataStorage[0].Address)
				assert.Equal(t, 4, cluster.DataStorage[0].Database)
			}
			assert.Equal(t, "192.168.58.149:2000", cluster.MetadataStorage.Address)
			assert.Equal(t, 8, cluster.MetadataStorage.Database)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			assert.Equal(t, uint64(4096), vdisk.BlockSize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.StorageCluster)
			assert.Equal(t, "rootcluster", vdisk.RootStorageCluster)
			assert.Equal(t, "tlogcluster", vdisk.TlogStorageCluster)
			assert.Equal(t, VdiskTypeBoot, vdisk.Type)
		}
	}
}

const minimalValidConfig = `
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: cache`

func TestMinimalValidConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(minimalValidConfig))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 1) {
		if cluster, ok := cfg.StorageClusters["mycluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0].Address)
				assert.Equal(t, 0, cluster.DataStorage[0].Database)
			}
			assert.Nil(t, cluster.MetadataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			assert.Equal(t, uint64(4096), vdisk.BlockSize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.StorageCluster)
			assert.Equal(t, "", vdisk.RootStorageCluster)
			assert.Equal(t, "", vdisk.TlogStorageCluster)
			assert.Equal(t, VdiskTypeCache, vdisk.Type)
		}
	}
}

var invalidConfigs = []string{
	// no storage clusters
	`
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no vdisks
	`storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
      - address: 192.123.123.123:2001
    metadataStorage:
      address: 192.168.58.146:2001
`,
	// no data storage given
	`
storageClusters:
  mycluster:
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// invalid data storage given
	`
storageClusters:
  mycluster:
    dataStorage:
      - foo
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2001
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no meta storage given (while deduped requires it)
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
      - address: 192.123.123.123:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no meta storage given (while deduped requires it)
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.123.123.123:2001
  rootcluster:
    dataStorage:
      - address: 192.168.58.146:2000
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    rootStorageCluster: rootcluster
    type: boot
`,
	// invalid meta storage given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
      - address: 192.123.123.123:2001
    metadataStorage: foo
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
      - address: 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2000
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
      - address: 192.123.123.123:2001
    metadataStorage:
      address: foo
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no block size given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no size given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    storageCluster: mycluster
    type: boot
`,
	// bad readOnly type given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    readOnly: foo
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no storage Cluster Name given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    type: boot
`,
	// bad vdisk type given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    type: foo
`,
	// unreferenced storageCluster given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: foo
    type: boot
`,
	// unreferenced rootStorageCluster given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    rootStorageCluster: foo
    type: boot
`,
	// unreferenced tlogStorageCluster given
	`
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
    metadataStorage:
      address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    tlogStorageCluster: foo
    type: boot
`,
}

func TestInvalidConfigFromBytes(t *testing.T) {
	for _, input := range invalidConfigs {
		cfg, err := FromBytes([]byte(input))
		if assert.Error(t, err, input) {
			assert.Nil(t, cfg)
		}
	}
}

var invalidVdiskTypes = []string{
	"foo",
	"123",
}
var validVDiskTypeCases = []struct {
	Input    string
	Expected VdiskType
}{
	{string(VdiskTypeBoot), VdiskTypeBoot},
	{string(VdiskTypeCache), VdiskTypeCache},
	{string(VdiskTypeDB), VdiskTypeDB},
	{string(VdiskTypeTmp), VdiskTypeTmp},
}

func TestValidVdiskTypeDeserialization(t *testing.T) {
	var vdiskType VdiskType
	for _, validCase := range validVDiskTypeCases {
		err := yaml.Unmarshal([]byte(validCase.Input), &vdiskType)
		if !assert.NoError(t, err, "unexpected invalid type: %q", validCase.Input) {
			continue
		}

		assert.Equal(t, validCase.Expected, vdiskType)
	}
}

func TestInvalidVdiskTypeDeserialization(t *testing.T) {
	var vdiskType VdiskType
	for _, invalidType := range invalidVdiskTypes {
		err := yaml.Unmarshal([]byte(invalidType), &vdiskType)
		assert.Error(t, err, "unexpected valid type: %q", invalidType)
	}
}

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

func TestParseInvalidCSStorageServerConfigStrings(t *testing.T) {
	testCases := []string{
		"foo",
		"localhost",
		"localhost:foo",
		"localhost1",
		"localhost:1@",
		"localhost:1@foo",
		"localhost:1localhost:2",
		"localhost:1,foo",
		"localhost:1,localhost",
		"localhost:1,localhost:foo",
		"localhost:1@foo,localhost:2",
		"localhost:1,localhost:2@foo",
	}

	for _, testCase := range testCases {
		_, err := ParseCSStorageServerConfigStrings(testCase)
		assert.Error(t, err, testCase)
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
