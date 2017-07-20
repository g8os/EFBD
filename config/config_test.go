package config

import (
	"strings"
	"testing"

	"github.com/go-yaml/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
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
  templatecluster:
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
    templateStorageCluster: templatecluster
    templateVdiskID: mytemplate
    tlogStorageCluster: tlogcluster
    type: db`

func TestValidConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(validConfig), Global)
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

		if cluster, ok := cfg.StorageClusters["templatecluster"]; assert.True(t, ok) {
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
			assert.Equal(t, "templatecluster", vdisk.TemplateStorageCluster)
			assert.Equal(t, "mytemplate", vdisk.TemplateVdiskID)
			assert.Equal(t, "tlogcluster", vdisk.TlogStorageCluster)
			assert.Equal(t, VdiskTypeDB, vdisk.Type)
		}
	}
}

func TestValidVdiskClusterConfigFromBytes(t *testing.T) {
	config, err := FromBytes([]byte(validConfig), Global)
	if !assert.NoError(t, err) || !assert.NotNil(t, config) {
		return
	}

	cfg, err := config.VdiskClusterConfig("myvdisk")
	if !assert.NoError(t, err) {
		return
	}

	// test Vdisk
	assert.Equal(t, uint64(4096), cfg.Vdisk.BlockSize)
	assert.False(t, cfg.Vdisk.ReadOnly)
	assert.Equal(t, uint64(10), cfg.Vdisk.Size)
	assert.Equal(t, "mycluster", cfg.Vdisk.StorageCluster)
	assert.Equal(t, "templatecluster", cfg.Vdisk.TemplateStorageCluster)
	assert.Equal(t, "mytemplate", cfg.Vdisk.TemplateVdiskID)
	assert.Equal(t, "tlogcluster", cfg.Vdisk.TlogStorageCluster)
	assert.Equal(t, VdiskTypeDB, cfg.Vdisk.Type)

	// test data cluster
	if assert.Len(t, cfg.DataCluster.DataStorage, 2) {
		assert.Equal(t, "192.168.58.146:2000", cfg.DataCluster.DataStorage[0].Address)
		assert.Equal(t, 0, cfg.DataCluster.DataStorage[0].Database)
		assert.Equal(t, "192.123.123.123:2001", cfg.DataCluster.DataStorage[1].Address)
		assert.Equal(t, 0, cfg.DataCluster.DataStorage[1].Database)
	}
	assert.Equal(t, "192.168.58.146:2001", cfg.DataCluster.MetadataStorage.Address)
	assert.Equal(t, 1, cfg.DataCluster.MetadataStorage.Database)

	// test template cluster
	if assert.Len(t, cfg.TemplateCluster.DataStorage, 1) {
		assert.Equal(t, "192.168.58.147:2000", cfg.TemplateCluster.DataStorage[0].Address)
		assert.Equal(t, 0, cfg.TemplateCluster.DataStorage[0].Database)
	}
	assert.Equal(t, "192.168.58.147:2001", cfg.TemplateCluster.MetadataStorage.Address)
	assert.Equal(t, 2, cfg.TemplateCluster.MetadataStorage.Database)

	// test tlog cluster
	if assert.Len(t, cfg.TlogCluster.DataStorage, 1) {
		assert.Equal(t, "192.168.58.149:2000", cfg.TlogCluster.DataStorage[0].Address)
		assert.Equal(t, 4, cfg.TlogCluster.DataStorage[0].Database)
	}
	assert.Equal(t, "192.168.58.149:2000", cfg.TlogCluster.MetadataStorage.Address)
	assert.Equal(t, 8, cfg.TlogCluster.MetadataStorage.Database)
}

func TestValidUniqueVdiskClusterConfigsFromBytes(t *testing.T) {
	config, err := FromBytes([]byte(validConfig), Global)
	if !assert.NoError(t, err) || !assert.NotNil(t, config) {
		return
	}

	// get the same vdisk cluster config twice,
	// and validate if all our expected pre-existing coditions exist

	cfgA, err := config.VdiskClusterConfig("myvdisk")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, cfgA.DataCluster.DataStorage, 2) {
		return
	}
	if !assert.NotNil(t, cfgA.DataCluster.MetadataStorage) {
		return
	}
	if !assert.NotNil(t, cfgA.TemplateCluster) {
		return
	}
	if !assert.NotNil(t, cfgA.TlogCluster) {
		return
	}

	cfgB, err := config.VdiskClusterConfig("myvdisk")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, cfgB.DataCluster.DataStorage, 2) {
		return
	}
	if !assert.NotNil(t, cfgB.DataCluster.MetadataStorage) {
		return
	}
	if !assert.NotNil(t, cfgB.TemplateCluster) {
		return
	}
	if !assert.NotNil(t, cfgB.TlogCluster) {
		return
	}

	dataStorageA := cfgA.DataCluster.DataStorage
	metaStorageA := cfgA.DataCluster.MetadataStorage
	templateDataStorageA := cfgA.TemplateCluster.DataStorage
	templateMetaStorageA := cfgA.TemplateCluster.MetadataStorage
	tlogDataStorageA := cfgA.TlogCluster.DataStorage
	tlogMetaStorageA := cfgA.TlogCluster.MetadataStorage

	dataStorageB := cfgB.DataCluster.DataStorage
	metaStorageB := cfgB.DataCluster.MetadataStorage
	templateDataStorageB := cfgB.TemplateCluster.DataStorage
	templateMetaStorageB := cfgB.TemplateCluster.MetadataStorage
	tlogDataStorageB := cfgB.TlogCluster.DataStorage
	tlogMetaStorageB := cfgB.TlogCluster.MetadataStorage

	assert.Equal(t, dataStorageA, dataStorageB)
	assert.Equal(t, *metaStorageA, *metaStorageB)
	assert.Equal(t, templateDataStorageA, templateDataStorageB)
	assert.Equal(t, *templateMetaStorageA, *templateMetaStorageB)
	assert.Equal(t, tlogDataStorageA, tlogDataStorageB)
	assert.Equal(t, *tlogMetaStorageA, *tlogMetaStorageB)

	// let's now change all A versions
	dataStorageA[0].Database++
	metaStorageA.Database++
	templateDataStorageA[0].Database++
	templateMetaStorageA.Database++
	tlogDataStorageA[0].Database++
	tlogMetaStorageA.Database++

	// now the versions shouldn't be equal
	assert.NotEqual(t, dataStorageA, dataStorageB)
	assert.NotEqual(t, *metaStorageA, *metaStorageB)
	assert.NotEqual(t, templateDataStorageA, templateDataStorageB)
	assert.NotEqual(t, *templateMetaStorageA, *templateMetaStorageB)
	assert.NotEqual(t, tlogDataStorageA, tlogDataStorageB)
	assert.NotEqual(t, *tlogMetaStorageA, *tlogMetaStorageB)
}

const minimalValidNBDServerConfig = `
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

func TestMinimalValidNBDServerConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(minimalValidNBDServerConfig), NBDServer)
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
			// required properties
			assert.Equal(t, uint64(4096), vdisk.BlockSize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.StorageCluster)
			assert.Equal(t, VdiskTypeCache, vdisk.Type)
			// optional properties
			assert.Equal(t, "", vdisk.TemplateStorageCluster)
			assert.Equal(t, "", vdisk.TemplateVdiskID)
			// ignored properties
			assert.Equal(t, "", vdisk.TlogStorageCluster)
		}
	}
}

const minimalValidNBDServerConfigWithTemplate = `
storageClusters:
  mycluster:
    dataStorage:
      - address: 192.168.58.146:2000
  templatecluster:
    dataStorage:
      - address: 192.168.58.146:2001
vdisks:
  myvdisk:
    blockSize: 4096
    size: 10
    storageCluster: mycluster
    templateStorageCluster: templatecluster
    type: db`

func TestMinimalValidNBDServerConfigWithTemplateFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(minimalValidNBDServerConfigWithTemplate), NBDServer)
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 2) {
		if cluster, ok := cfg.StorageClusters["mycluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0].Address)
				assert.Equal(t, 0, cluster.DataStorage[0].Database)
			}
			assert.Nil(t, cluster.MetadataStorage)
		}

		if cluster, ok := cfg.StorageClusters["templatecluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 1) {
				assert.Equal(t, "192.168.58.146:2001", cluster.DataStorage[0].Address)
				assert.Equal(t, 0, cluster.DataStorage[0].Database)
			}
			assert.Nil(t, cluster.MetadataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			// required properties
			assert.Equal(t, uint64(4096), vdisk.BlockSize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(10), vdisk.Size)
			assert.Equal(t, "mycluster", vdisk.StorageCluster)
			assert.Equal(t, VdiskTypeDB, vdisk.Type)
			// optional properties
			assert.Equal(t, "templatecluster", vdisk.TemplateStorageCluster)
			assert.Equal(t, "myvdisk", vdisk.TemplateVdiskID)
			// ignored properties
			assert.Equal(t, "", vdisk.TlogStorageCluster)
		}
	}
}

const minimalValidTlogServerConfig = `
storageClusters:
  tlogcluster:
    dataStorage:
      - address: 192.168.58.146:2000
      - address: 192.168.58.146:2001
      - address: 192.168.58.146:2002
      - address: 192.168.58.146:2003
vdisks:
  myvdisk:
    tlogStorageCluster: tlogcluster`

func TestMinimalValidTlogServerConfigFromBytes(t *testing.T) {
	cfg, err := FromBytes([]byte(minimalValidTlogServerConfig), TlogServer)
	if !assert.NoError(t, err) || !assert.NotNil(t, cfg) {
		return
	}

	if assert.Len(t, cfg.StorageClusters, 1) {
		if cluster, ok := cfg.StorageClusters["tlogcluster"]; assert.True(t, ok) {
			if assert.Len(t, cluster.DataStorage, 4) {
				assert.Equal(t, "192.168.58.146:2000", cluster.DataStorage[0].Address)
				assert.Equal(t, 0, cluster.DataStorage[0].Database)
				assert.Equal(t, "192.168.58.146:2001", cluster.DataStorage[1].Address)
				assert.Equal(t, 0, cluster.DataStorage[1].Database)
				assert.Equal(t, "192.168.58.146:2002", cluster.DataStorage[2].Address)
				assert.Equal(t, 0, cluster.DataStorage[2].Database)
				assert.Equal(t, "192.168.58.146:2003", cluster.DataStorage[3].Address)
				assert.Equal(t, 0, cluster.DataStorage[3].Database)
			}
			assert.Nil(t, cluster.MetadataStorage)
		}
	}

	if assert.Len(t, cfg.Vdisks, 1) {
		if vdisk, ok := cfg.Vdisks["myvdisk"]; assert.True(t, ok) {
			// required properties
			assert.Equal(t, "tlogcluster", vdisk.TlogStorageCluster)
			// ignored properties
			assert.Equal(t, uint64(0), vdisk.BlockSize)
			assert.False(t, vdisk.ReadOnly)
			assert.Equal(t, uint64(0), vdisk.Size)
			assert.Equal(t, "", vdisk.TemplateStorageCluster)
			assert.Equal(t, "", vdisk.StorageCluster)
			assert.Equal(t, VdiskType(0), vdisk.Type)
		}
	}
}

func TestMinimalValidVdiskClusterNBDServerConfigFromBytes(t *testing.T) {
	config, err := FromBytes([]byte(minimalValidNBDServerConfig), NBDServer)
	if !assert.NoError(t, err) || !assert.NotNil(t, config) {
		return
	}

	cfg, err := config.VdiskClusterConfig("myvdisk")
	if !assert.NoError(t, err) {
		return
	}

	// test Vdisk
	// required properties
	assert.Equal(t, uint64(4096), cfg.Vdisk.BlockSize)
	assert.False(t, cfg.Vdisk.ReadOnly)
	assert.Equal(t, uint64(10), cfg.Vdisk.Size)
	assert.Equal(t, "mycluster", cfg.Vdisk.StorageCluster)
	assert.Equal(t, VdiskTypeCache, cfg.Vdisk.Type)
	// ignored properties
	assert.Equal(t, "", cfg.Vdisk.TemplateStorageCluster)
	assert.Equal(t, "", cfg.Vdisk.TlogStorageCluster)

	// test data cluster
	if assert.Len(t, cfg.DataCluster.DataStorage, 1) {
		assert.Equal(t, "192.168.58.146:2000", cfg.DataCluster.DataStorage[0].Address)
		assert.Equal(t, 0, cfg.DataCluster.DataStorage[0].Database)
	}
	assert.Nil(t, cfg.DataCluster.MetadataStorage)

	// template and tlog clusters are not defined
	assert.Nil(t, cfg.TemplateCluster)
	assert.Nil(t, cfg.TlogCluster)
}

func TestMinimalValidVdiskClusterTlogServerConfigFromBytes(t *testing.T) {
	config, err := FromBytes([]byte(minimalValidTlogServerConfig), TlogServer)
	if !assert.NoError(t, err) || !assert.NotNil(t, config) {
		return
	}

	cfg, err := config.VdiskClusterConfig("myvdisk")
	if !assert.NoError(t, err) {
		return
	}

	// test Vdisk
	// required properties
	assert.Equal(t, "tlogcluster", cfg.Vdisk.TlogStorageCluster)
	// ignored properties
	assert.Equal(t, "", cfg.Vdisk.StorageCluster)
	assert.Equal(t, "", cfg.Vdisk.TemplateStorageCluster)
	assert.Equal(t, uint64(0), cfg.Vdisk.BlockSize)
	assert.False(t, cfg.Vdisk.ReadOnly)
	assert.Equal(t, uint64(0), cfg.Vdisk.Size)
	assert.Equal(t, VdiskType(0), cfg.Vdisk.Type)

	if !assert.NotNil(t, cfg.TlogCluster) {
		return
	}

	// test data cluster
	if assert.Len(t, cfg.TlogCluster.DataStorage, 4) {
		assert.Equal(t, "192.168.58.146:2000", cfg.TlogCluster.DataStorage[0].Address)
		assert.Equal(t, 0, cfg.TlogCluster.DataStorage[0].Database)
		assert.Equal(t, "192.168.58.146:2001", cfg.TlogCluster.DataStorage[1].Address)
		assert.Equal(t, 0, cfg.TlogCluster.DataStorage[1].Database)
		assert.Equal(t, "192.168.58.146:2002", cfg.TlogCluster.DataStorage[2].Address)
		assert.Equal(t, 0, cfg.TlogCluster.DataStorage[2].Database)
		assert.Equal(t, "192.168.58.146:2003", cfg.TlogCluster.DataStorage[3].Address)
		assert.Equal(t, 0, cfg.TlogCluster.DataStorage[3].Database)
	}
	assert.Nil(t, cfg.TlogCluster.MetadataStorage)

	// template and tlog clusters are not defined
	assert.Nil(t, cfg.DataCluster)
	assert.Nil(t, cfg.TemplateCluster)
}

var invalidNBDServerConfigs = []string{
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
	// unreferenced templateStorageCluster given
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
    templateStorageCluster: foo
    type: boot
`,
}

func TestInvalidNBDServerConfigFromBytes(t *testing.T) {
	for _, input := range invalidNBDServerConfigs {
		cfg, err := FromBytes([]byte(input), NBDServer)
		if assert.Error(t, err, input) {
			assert.Nil(t, cfg)
		}
	}
}

var invalidTlogServerConfigs = []string{
	// no tlogStorageCluster given
	`
vdisks:
  myvdisk:
    type: boot
`,
	// unreferenced tlogStorageCluster given
	`
vdisks:
  myvdisk:
    tlogStorageCluster: foo
`,
	// bad templateStorageCluster given
	`
storageClusters:
  tlogcluster:
    foo: bar
vdisks:
  myvdisk:
    tlogStorageCluster: tlogcluster
`,
	// bad templateStorageCluster given
	`
storageClusters:
  tlogcluster:
    address: foo
vdisks:
  myvdisk:
    tlogStorageCluster: tlogcluster
`,
	// bad (v2) templateStorageCluster given
	`
storageClusters:
  tlogcluster:
    address: localhost:6379
    db: foo
vdisks:
  myvdisk:
    tlogStorageCluster: tlogcluster
`,
}

func TestInvalidTlogServerConfigFromBytes(t *testing.T) {
	for _, input := range invalidTlogServerConfigs {
		cfg, err := FromBytes([]byte(input), NBDServer)
		if assert.Error(t, err, input) {
			assert.Nil(t, cfg)
		}
	}
}

func TestInvalidConfigFromBytes(t *testing.T) {
	invalidConfigs := append(invalidNBDServerConfigs, invalidTlogServerConfigs...)
	for _, input := range invalidConfigs {
		cfg, err := FromBytes([]byte(input), Global)
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
	String string
	Type   VdiskType
}{
	{vdiskTypeBootStr, VdiskTypeBoot},
	{vdiskTypeCacheStr, VdiskTypeCache},
	{vdiskTypeDBStr, VdiskTypeDB},
	{vdiskTypeTmpStr, VdiskTypeTmp},
}

func TestVdiskTypeValidate(t *testing.T) {
	for _, validCase := range validVDiskTypeCases {
		assert.NoError(t, validCase.Type.Validate())
	}

	assert.Error(t, VdiskType(0).Validate())
	assert.Error(t, VdiskType(255).Validate())
}

func TestValidVdiskTypeDeserialization(t *testing.T) {
	var vdiskType VdiskType
	for _, validCase := range validVDiskTypeCases {
		err := yaml.Unmarshal([]byte(validCase.String), &vdiskType)
		if !assert.NoError(t, err, "unexpected invalid type: %q", validCase.String) {
			continue
		}

		assert.Equal(t, validCase.Type, vdiskType)
	}
}

func TestValidVdiskTypeSerialization(t *testing.T) {
	for _, validCase := range validVDiskTypeCases {
		bytes, err := yaml.Marshal(validCase.Type)
		if !assert.NoError(t, err) {
			continue
		}

		str := strings.Trim(string(bytes), "\n")
		assert.Equal(t, validCase.String, str)
	}
}

func TestInvalidVdiskTypeDeserialization(t *testing.T) {
	var vdiskType VdiskType
	for _, invalidType := range invalidVdiskTypes {
		err := yaml.Unmarshal([]byte(invalidType), &vdiskType)
		assert.Error(t, err, "unexpected valid type: %q", invalidType)
	}
}

func TestVdiskProperties(t *testing.T) {
	v := func(t VdiskType) *VdiskConfig {
		return &VdiskConfig{Type: t}
	}

	// validate storage type property
	assert.Equal(t, StorageDeduped, v(VdiskTypeBoot).StorageType())
	assert.Equal(t, StorageNonDeduped, v(VdiskTypeDB).StorageType())
	assert.Equal(t, StorageNonDeduped, v(VdiskTypeCache).StorageType())
	assert.Equal(t, StorageNonDeduped, v(VdiskTypeTmp).StorageType())

	// validate tlog support
	assert.True(t, v(VdiskTypeBoot).TlogSupport())
	assert.True(t, v(VdiskTypeDB).TlogSupport())
	assert.False(t, v(VdiskTypeCache).TlogSupport())
	assert.False(t, v(VdiskTypeTmp).TlogSupport())

	// validate template support
	assert.True(t, v(VdiskTypeBoot).TemplateSupport())
	assert.True(t, v(VdiskTypeDB).TemplateSupport())
	assert.False(t, v(VdiskTypeCache).TemplateSupport())
	assert.False(t, v(VdiskTypeTmp).TemplateSupport())
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

func init() {
	log.SetLevel(log.DebugLevel)
}
