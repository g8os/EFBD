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
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
  rootcluster:
    dataStorage:
      - 192.168.58.147:2000
    metadataStorage: 192.168.58.147:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storageCluster: mycluster
    rootStorageCluster: rootcluster
    tlogStorageCluster: ''
    type: boot`

func TestValidConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(validConfig))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 2) {
		if cluster, ok := cfg.StorageClusters["mycluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 2) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0])
				assert.Equal(t, "192.123.123.123:2001", cluster.DataStorage[1])
			}
			assert.Equal(t, "192.168.58.146:2001", cluster.MetaDataStorage)
		}

		if cluster, ok := cfg.StorageClusters["rootcluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.147:2000", cluster.DataStorage[0])
			}
			assert.Equal(t, "192.168.58.147:2001", cluster.MetaDataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			assert.Equal(t, uint64(4096), vdisk.Blocksize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.Storagecluster)
			assert.Equal(t, "rootcluster", vdisk.RootStorageCluster)
			assert.Equal(t, "", vdisk.TlogStoragecluster)
			assert.Equal(t, VdiskTypeBoot, vdisk.Type)
		}
	}
}

const minimalValidConfig = `
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    type: boot`

func TestMinimalValidConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(minimalValidConfig))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 1) {
		if cluster, ok := cfg.StorageClusters["mycluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0])
			}
			assert.Equal(t, "192.168.58.146:2001", cluster.MetaDataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			assert.Equal(t, uint64(4096), vdisk.Blocksize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.Storagecluster)
			assert.Equal(t, "", vdisk.RootStorageCluster)
			assert.Equal(t, "", vdisk.TlogStoragecluster)
			assert.Equal(t, VdiskTypeBoot, vdisk.Type)
		}
	}
}

var invalidConfigs = []string{
	// no storage clusters
	`
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no vdisks
	`storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
`,
	// no data storage given
	`
storageClusters:
  mycluster:
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
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
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no meta storage given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// invalid meta storage given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: foo
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    type: boot
`,
	// no block size given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
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
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    storageCluster: mycluster
    type: boot
`,
	// bad readOnly type given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
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
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    type: boot
`,
	// bad vdisk type given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    type: foo
`,
	// unreferenced storageCluster given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: foo
    type: boot
`,
	// unreferenced rootStorageCluster given
	`
storageClusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storageCluster: mycluster
    rootStorageCluster: foo
    type: boot
`,
}

func TestInvalidConfigFromBytes(t *testing.T) {
	for _, input := range invalidConfigs {
		cfg, err := FromBytes([]byte(input))
		if assert.Error(t, err) {
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
