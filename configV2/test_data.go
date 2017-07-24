package configV2

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
    - address: 192.168.2.149:1000
      db: 4
  metadataStorage:
    address: 192.168.2.146:1001
    db: 8
`

var invalidNBDServerConfigs = []string{
	// invalid blocksize (0)
	`
baseConfig: 
  blockSize: 0
  readOnly: false
  size: 1
  type: db
  `,
	// invalid blocksize (uneven)
	`
baseConfig: 
  blockSize: 4095
  readOnly: false
  size: 2
  type: db
  `,
	// invalid VdiskType (nil)
	`
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 3
  type: 
  `,
	// invalid blocksize (random string)
	`
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 4
  type: random
  `,
	// no storage clusters
	`
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 5
  type: db
nbdConfig:
  templateVdiskID: mytemplate
`,
	// missing template storage cluster
	`
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 6
  type: db
nbdConfig:
  templateVdiskID: mytemplate
  storageCluster:
    dataStorage: 
      - address: 192.168.58.146:2000
        db: 0
      - address: 192.123.123.123:2001
        db: 1
    metadataStorage:
      address: 192.168.58.146:2001
      db: 1
  `,

	// bad template storage
	`
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 7
  type: tmp
nbdConfig:
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
  `,
	// bad template storage dial address
	`
baseConfig: 
  blockSize: 4096
  readOnly: false
  size: 8
  type: tmp
nbdConfig:
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
  `,
}
