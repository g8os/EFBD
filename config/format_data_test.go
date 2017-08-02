package config

var validNBDVdisksConfigYAML = []string{
	// one vdisk
	`
vdisks:
  - foo
`, // multiple vdisks
	`
vdisks:
  - foo
  - bar
`,
}

var invalidNBDVdisksConfigYAML = []string{
	"",
	`
foo: bar
`,
	`
foo:
  - bar
`,
}

var validVdiskStaticConfigYAML = []string{
	// prime config example
	`
blockSize: 4096
size: 10
readOnly: false
type: boot
`, // minimal version of the example above
	`
blockSize: 4096
size: 10
type: boot
`, // another valid eample
	`
blockSize: 512
size: 1
type: db
`, // yet another valid one
	`
blockSize: 2048
size: 2
type: cache
`, // yet another valid one
	`
blockSize: 8192
size: 5
type: tmp
`,
}

var invalidVdiskStaticConfigYAML = []string{
	"",
	// blockSize not given
	`
size: 10
type: boot
`, // blockSize has to be power of 2
	`
blockSize: 1000
size: 10
type: boot
`, // size has to be given
	`
blockSize: 512
type: boot
`, // size < blockSize/bytes
	`
blockSize: 512
size: 0
type: boot
`, // readOnly bad value
	`
blockSize: 4096
size: 10
readOnly: foo
type: boot
`, // type not given
	`
blockSize: 4096
size: 10
`, // type bad value
	`
blockSize: 4096
size: 10
type: foo
`,
}

var validVdiskNBDConfigYAML = []string{
	// complete example
	`
storageClusterID: foo
templateStorageClusterID: bar
templateVdiskID: template
tlogServerClusterID: baz
`, // minimal example of above
	`
storageClusterID: bar
`, // more variations...
	`
storageClusterID: baz
templateStorageClusterID: foo
`, `
storageClusterID: baz
templateStorageClusterID: bar
templateVdiskID: foo
`, `
storageClusterID: baz
tlogServerClusterID: foo
`,
}

var invalidVdiskNBDConfigYAML = []string{
	"",
	// storageClusterID not given
	`
templateStorageClusterID: foo
templateVdiskID: bar
`,
}

var validVdiskTlogConfigYAML = []string{
	// complete example
	`
storageClusterID: foo
slaveStorageClusterID: bar
`, // minimal example of above
	`
storageClusterID: foo
`, // more variations...
	`
storageClusterID: bar
slaveStorageClusterID: foo
`,
}

var invalidVdiskTlogConfigYAML = []string{
	"",
	// storageClusterID not given
	`
slaveStorageClusterID: bar
`,
}

var validStorageClusterConfigYAML = []string{
	// complete example
	`
dataStorage:
  - address: 1.1.1.1:11
    db: 1
  - address: 2.2.2.2:22
    db: 2
metadataStorage:
  address: 3.3.3.3:33
  db: 3
`, // similar example from above, but using ipv6
	`
dataStorage:
  - address: "[2001:db8:0:1:1:1:1:1]:11"
    db: 1
  - address: "[2001:db8:0:2:2:2:2:2]:22"
    db: 2
metadataStorage:
  address: "[2001:db8:0:3:3:3:3:3]:33"
  db: 3
`, // most minimal version of 1 example
	`
dataStorage:
  - address: 1.1.1.1:11
`, // most minimal (local) version of 1 example
	`
dataStorage:
  - address: localhost:16379
`, // other variations of first example...
	`
dataStorage:
  - address: 1.1.1.1:11
    db: 1
  - address: 2.2.2.2:22
`, `
dataStorage:
  - address: 1.1.1.1:11
  - address: 2.2.2.2:22
    db: 2
metadataStorage:
  address: 3.3.3.3:33
`, `
dataStorage:
  - address: 1.1.1.1:11
metadataStorage:
  address: 3.3.3.3:33
`,
}

var invalidStorageClusterConfigYAML = []string{
	"",
	// dataStorage not given
	`
metadataStorage:
  address: 3.3.3.3:33
`, // dataStorage given but doesn't contain a single server
	`
dataStorage:
  foo: bar
`, // dataStorage given but doesn't define the dial string
	`
dataStorage:
  - db: 2
`, // dataStorage given but wrong type, requires array of storage servers,
	// instead a single storage server is given directly
	`
dataStorage:
  address: localhost:16379
`, // dataStorage given but invalid metadataStorage
	`
dataStorage:
  - address: localhost:16379
metadataStorage:
  db: 2
`,
}

var validTlogServerConfigYAML = []string{
	`address: 127.0.0.1:42`,
	`address: localhost:16379`,
	`address: "[2001:db8:0:3:3:3:3:3]:33"`,
}

var invalidTlogServerConfigYAML = []string{
	"",
	"foo: bar",
	"address: bar",
	"address: localhost",
	"localhost:16379",
	// valid, but because of the IPv6 formatting
	// it requires quotes
	"address: [2001:db8:0:3:3:3:3:3]:33",
}

var validStorageServerConfigYAML = append(validTlogServerConfigYAML, []string{
	// complete example (1)
	`
address: localhost:16379
db: 1
`, // complete example (2)
	`
address: 127.0.0.1:16379
db: 2
`, // complete example (1)
	`
address: "[2001:db8:0:3:3:3:3:3]:33"
db: 3
`, // minimal version of first example (1)
	`
   address: localhost:16379
`, // minimal version of first example (2)
	`
   address: "[2001:db8:0:3:3:3:3:3]:33"
`,
}...)

var invalidStorageServerConfigYAML = append(invalidTlogServerConfigYAML, []string{
	// only db given
	`
db: 3
`,
}...)
