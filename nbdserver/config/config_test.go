package config

import (
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
)

const validConfig = `
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot`

func TestValidConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(validConfig))
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 1) {
		if cluster, ok := cfg.StorageClusters["mycluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 2) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0])
				assert.Equal(t, "192.123.123.123:2001", cluster.DataStorage[1])
			}
			assert.Equal(t, "192.168.2.2:2002", cluster.RootDataStorage)
			assert.Equal(t, "192.168.58.146:2001", cluster.MetaDataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			assert.Equal(t, uint64(4096), vdisk.Blocksize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.Storagecluster)
			assert.Equal(t, "", vdisk.TlogStoragecluster)
			assert.Equal(t, VdiskTypeBoot, vdisk.Type)
		}
	}
}

const minimalValidConfig = `
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    size: 10
    storagecluster: mycluster
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
			assert.Equal(t, "", cluster.RootDataStorage)
			assert.Equal(t, "192.168.58.146:2001", cluster.MetaDataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			assert.Equal(t, uint64(4096), vdisk.Blocksize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.Storagecluster)
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
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// no vdisks
	`storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
`,
	// no data storage given
	`
storageclusters:
  mycluster:
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// invalid data storage given
	`
storageclusters:
  mycluster:
    dataStorage:
      - foo
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// invalid root data storage given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: foo
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// no meta storage given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// invalid meta storage given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: foo
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// no block size given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// no size given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// bad readOnly type given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: foo
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: boot
`,
	// no storage Cluster Name given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    tlogStoragecluster: ''
    type: boot
`,
	// bad vdisk type given
	`
storageclusters:
  mycluster:
    dataStorage:
      - 192.168.58.146:2000
      - 192.123.123.123:2001
    rootDataStorage: 192.168.2.2:2002
    metadataStorage: 192.168.58.146:2001
vdisks:
  myvdisk:
    blocksize: 4096
    readOnly: false
    size: 10
    storagecluster: mycluster
    tlogStoragecluster: ''
    type: foo
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
