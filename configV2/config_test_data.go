package configV2

const validYAMLSourceStr = `
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 10
  type: db
nbdConfig:
  templateVdiskID: mytemplate
  storageCluster:
    dataStorage: 
      - address: 192.168.58.146:2000
        db: 0
      - address: 192.123.123.123:2001
        db: 0
    metadataStorage:
      address: 192.168.58.146:2001
      db: 1
  templateStorageCluster:
    dataStorage:
      - address: 192.168.58.147:2000
        db: 0
tlogConfig:
  tlogStorageCluster:
    dataStorage: 
      - address: 192.168.58.149:2000
        db: 4
    metadataStorage:
      address: 192.168.58.146:2001
      db: 8
  `
const validBaseStr = `
blockSize: 2048
readOnly: true
size: 110
type: cache
`
const validNBDStr = `
templateVdiskID: mytemplate
storageCluster:
  dataStorage: 
    - address: 192.168.1.146:2000
      db: 10
    - address: 192.123.123.1:2001
      db: 10
  metadataStorage:
    address: 192.168.1.146:2001
    db: 11
templateStorageCluster:
  dataStorage:
    - address: 192.168.1.147:2000
      db: 10
`

const validTlogStr = `
tlogStorageCluster:
  dataStorage: 
    - address: 192.168.1.1:1000
      db: 14
  metadataStorage:
    address: 192.168.1.1:1001
    db: 18
tlogSlaveSync: true
`
const validSlaveStr = `
slaveStorageCluster:
  dataStorage: 
    - address: 192.168.58.149:2000
      db: 4
  metadataStorage:
    address: 192.168.58.146:2001
    db: 8
`

var validBaseConfig = BaseConfig{
	BlockSize: 1234,
	ReadOnly:  false,
	Size:      15,
	Type:      VdiskTypeBoot,
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

var invalidNBDServerConfigs = []string{
	// no storage clusters
	`
vdisks:
  myvdisk:
    baseConfig:
      blockSize: 4096
      size: 1
      type: boot
`,
	// missing template storage cluster
	`
vdisks:
  myvdisk:
    baseConfig: 
      blockSize: 4096
      readOnly: false
      size: 2
      type: db
    ndbConfig:
      templateVdiskID: mytemplate
      storageCluster:
        dataStorage: 
          - address: 192.168.58.146:2000
            db: 0
          - address: 192.123.123.123:2001
            db: 0
        metadataStorage:
          address: 192.168.58.146:2001
          db: 1
    tlogConfig:
      tlogStorageCluster:
        dataStorage: 
          - address: 192.168.58.149:2000
            db: 4
        metadataStorage:
          address: 192.168.58.146:2001
          db: 8
  `,

	// bad template storage
	`
vdisks:
  myvdisk:
    baseConfig: 
      blockSize: 4096
      readOnly: false
      size: 3
      type: tmp
    ndbConfig:
      storageCluster:
        dataStorage: 
          - address: 192.168.58.146:2000
            db: 0
          - address: 192.123.123.123:2001
            db: 0
        metadataStorage:
          address: 192.168.58.146:2001
          db: 1
      templateStorageCluster:
        dataStorage:
          - address: 192.168.58.147:2000
            db: foo
    tlogConfig:
      tlogStorageCluster:
        dataStorage: 
          - address: 192.168.58.149:2000
            db: 4
        metadataStorage:
          address: 192.168.58.146:2001
          db: 8
  `,
	// bad template storage dial address
	`
vdisks:
  myvdisk:
    baseConfig: 
      blockSize: 4096
      readOnly: false
      size: 3
      type: tmp
    ndbConfig:
      storageCluster:
        dataStorage: 
          - address: 192.168.58.146:2000
            db: 0
          - address: 192.123.123.123:2001
            db: 0
        metadataStorage:
          address: 192.168.58.146:2001
          db: 1
      templateStorageCluster:
        dataStorage:
          - address: foo
            db: 1
    tlogConfig:
      tlogStorageCluster:
        dataStorage: 
          - address: 192.168.58.149:2000
            db: 4
        metadataStorage:
          address: 192.168.58.146:2001
          db: 8
  `,
}
