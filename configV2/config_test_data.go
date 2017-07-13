package configV2

// YAML

const validConfigStr = `
vdisks:
  myvdisk:
    baseConfig: 
      blockSize: 4096
      readOnly: false
      size: 10
      type: db
    ndbConfig:
      rootVdiskID: mytemplate
      storageCluster:
        dataStorage: 
          - address: 192.168.58.146:2000
            db: 0
          - address: 192.123.123.123:2001
            db: 0
        metadataStorage:
          address: 192.168.58.146:2001
          db: 1
      rootStorageCluster:
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
blockSize: 4096
readOnly: false
size: 10
type: db
`
const validNBDStr = `
rootVdiskID: mytemplate
storageCluster:
  dataStorage: 
    - address: 192.168.58.146:2000
      db: 0
    - address: 192.123.123.123:2001
      db: 0
  metadataStorage:
    address: 192.168.58.146:2001
    db: 1
rootStorageCluster:
  dataStorage:
    - address: 192.168.58.147:2000
      db: 0
`

const validTlogStr = `
tlogStorageCluster:
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
	// missing root storage cluster
	`
vdisks:
  myvdisk:
    baseConfig: 
      blockSize: 4096
      readOnly: false
      size: 2
      type: db
    ndbConfig:
      rootVdiskID: mytemplate
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

	// bad root storage
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
      rootStorageCluster:
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
	// bad root storage dail address
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
      rootStorageCluster:
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
