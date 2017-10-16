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
zeroStorClusterID: foo
slaveStorageClusterID: bar
`, // minimal example of above
	`
zeroStorClusterID: foo
`, // more variations...
	`
slaveStorageClusterID: foo
zeroStorClusterID: bar
`,
}

var invalidVdiskTlogConfigYAML = []string{
	"",
	// ZeroStorClusterID not given
	`
slaveStorageClusterID: bar
`,
}

var validStorageClusterConfigYAML = []string{
	// complete example
	`
servers:
  - address: 1.1.1.1:11
    db: 1
  - address: 2.2.2.2:22
    db: 2
`, // similar example from above, but using ipv6
	`
servers:
  - address: "[2001:db8:0:1:1:1:1:1]:11"
    db: 1
  - address: "[2001:db8:0:2:2:2:2:2]:22"
    db: 2
`, // most minimal version of 1 example
	`
servers:
  - address: 1.1.1.1:11
`, // most minimal (local) version of 1 example
	`
servers:
  - address: localhost:16379
`, // other variations of first example...
	`
servers:
  - address: 1.1.1.1:11
    db: 1
  - address: 2.2.2.2:22
`, `
servers:
  - address: 1.1.1.1:11
  - address: 2.2.2.2:22
    db: 2
`,
	// the most minimal cluster config
	`
servers:
  - address: 1.1.1.1:11
`, // only disabled servers given
	`
servers:
  - address: localhost:16379
    state: rip
  - state: rip
`,
}

var invalidStorageClusterConfigYAML = []string{
	"",
	// dataStorage given but doesn't contain a single server
	`
servers:
  foo: bar
`, // dataStorage given but doesn't define the dial string
	`
servers:
  - db: 2
`, // dataStorage given but wrong type, requires array of storage servers,
	// instead a single storage server is given directly
	`
servers:
  address: localhost:16379
`,
}

var validStorageServerConfigYAML = []string{
	// complete example (1)
	`
address: localhost:16379
db: 1
state: online
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
	// when state is RIP, the other properties are irrelevant
	`
state: rip
`,
}

var invalidStorageServerConfigYAML = []string{
	// only db given
	`
db: 3
`,
	// invalid db given
	`
address: localhost:16379
db: -1
`,
}

var validZeroStorClusterConfigYAML = []string{
	// complete example
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
  - address: "2.2.2.2:22"
metadataServers:
  - address: "3.3.3.3:33"
`, // minimal example
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
metadataServers:
  - address: "2.2.2.2:22"
`, // using unix socket address for metadata example
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
metadataServers:
  - address: "unix://etcd137925510"
`,
}

var invalidZeroStorClusterConfigYAML = []string{
	``, // missing org
	`
iyo:
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
metadataServers:
  - address: "3.3.3.3:33"
`, // missing namespace
	`
iyo:
  org: "foo org"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
metadataServers:
  - address: "3.3.3.3:33"
`, // missing server addresses
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
metadataServers:
  - address: "3.3.3.3:33"
`, // empty server address
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: ""
metadataServers:
  - address: "3.3.3.3:33"
`, // invalid server address
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "foo address"
metadataServers:
  - address: "3.3.3.3:33"
`,
	// missing metadataserver addresses
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
`, // empty metadataserver address
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
metadataServers:
  - address: ""
`, // invalid metadataserver address
	`
iyo:
  org: "foo org"
  namespace: "foo namespace"
  clientID: "foo client"
  secret: "foo secret"
servers:
  - address: "1.1.1.1:11"
metadataServers:
  - address: "unixfooaddress"
`,
}
