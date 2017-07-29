# 0-Disk Configuration

All 0-Disk services are configured using a YAML config source, written by the [0-orchestrator][orchestrator].

More technical details can be found in the [Godocs][configGodoc]

The config data is stored in 4 subconfigs: BaseConfig, NBDConfig, TlogConfig and SlaveConfig. The information is stored (in sources) and processed internally in YAML format.

Each subconfig has a constructor, ToBytes and a Validate methode.
The constructor will unmarshal a subconfig from on a YAML formatted slice of byte, validates the subconfig and returns an error if invalid.
ToBytes will marchal the subconfig to a YAML slice of bytes and return it.
Validate will validate the subconfig. It is however only recommended to be used before setting a subconfig to a source (e.g. etcd) in that source implementation for the configs.

The data is split up in this way so that for the use cases of 0-Disk, most of the time only one subconfig is required. Should another subconfig be required, it can easily be fetched from a source and stored together with the other subconfigs.

* NBD Server uses: BaseConfig, NBDConfig, SlaveConfig
* Tlog Server (server.vdisk) uses: TlogConfig

The source implementations of the config (etcd) allows for data being read from the source and include a watch/update feature that returns an updated subconfig as it is updated by the 0-orchestrator. This feature is not implemented for BaseConfig as that data should be quite static.

## Subconfigs
### Base config
* BaseConfig: stores general vdisk information:
	* BlockSize: Size of a block on the vdisk
	* ReadOnly: Defines if vdisk is readonly
	* Size: vdisk size
	* Type: Type of vdisk (Boot, DB, Cache, tmp)
	* TlogServerAddresses: Defines a list of network addresses of tlogserver(s)

* YAML example:
```yaml
blockSize: 4096 # can not be zero or uneven
readOnly: false
size: 10 	# can not be 0
type: db	# should be a valid vdisk type (boot, db, cache or tmp)
tlogServerAddresses:
  - 192.168.58.123:2000
  - 192.168.58.125:2000
```

* Used by:
	* [NBD Server][nbdServerConfig]:
		* ardb uses it for the BackendFactory

* Godoc: [BaseConfig][baseconfigGodoc]

### NBD config
* NBDConfig: stores NBD server information:
	* StorageCluster: defines the storage cluster of the NBD Server
	* TemplateStorageCluster: defines the template storage cluster of the NBD Server
	* TemplateVdiskID: defines the vdisk id of the template

* YAML example:
```yaml
templateVdiskID: testtemplate
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
```

* Used by:
	* [NBD Server][nbdServerConfig]:
		* ardb uses it for the BackendFactory

* Godoc: [NBDConfig][nbdconfigGodoc]

### Tlog config
* TlogConfig: stores tlog server information:
	* TlogStorageCluster: defines the storage cluster of the Tlog Server
	* SlaveSync: defines if tlog should use the slave syncer

* YAML example:
```yaml
tlogStorageCluster:
  dataStorage: 
    - address: 192.168.1.1:1000
      db: 14
  metadataStorage:
    address: 192.168.1.1:1001
    db: 18
tlogSlaveSync: true
```

* Used by:
	* [Tlog Server][tlogServerConfig]:
		* server.vdisk uses the tlogSlaveSync
		* slavesync uses the TlogStorageCluster

* Godoc: [TlogConfig][tlogconfigGodoc]

### Slave config
* SlaveConfig: stores slave storage cluster information:
	* SlaveStorageCluster: defines the storage cluster of the slave

* YAML example:
```yaml
slaveStorageCluster:
  dataStorage: 
    - address: 192.168.2.149:1000
      db: 14
  metadataStorage:
    address: 192.168.2.146:1001
    db: 18
```

* Used by:
	* [NBD Server][nbdServerConfig]:
		* nbdserver.tlog uses it for switching to ardb slave (switchToArdbSlave())

* Godoc: [SlaveConfig][slaveconfigGodoc]

## etcd
[etcd][etcd] is a distributed reliable key-value store and uses the Raft consensus algorithm to manage a highly-available replicated log. Support for etcd is added to ba able to store the configurations in a distributed way.

### Reading data
The source implementation for etcd provides methodes to read subconfigs from an etcd cluster. 
In the etcd cluster it will look for the following keys for the subconfigs:

```
BaseConfig: <VdiskID>:conf:base
NBDConfig: <VdiskID>:conf:nbd
TlogConfig: <VdiskID>:conf:tlog
SlaveConfig: <VdiskID>:conf:slave
```

The values stored in those keys are the subconfigs serialised in YAML format. This can be done using the subconfig's ToBytes() method.

When the values are read from etcd, the subconfig will be created with the subconfig's constructor, which will also check if the provided byte slice is valid.

e.g.:
```go
func ReadBaseConfigETCD(vdiskID string, endpoints []string) (*BaseConfig, error)
```
ReadBaseConfigETCD will read a Baseconfig looking on an etcd cluster from provided endpoint(s) that is parth of the cluster. It will then look for the following key ```<VdiskID>:conf:base``` generated with the provided vdiskID. If the vdiskID is ```vdisk123``` then it will look for the following key: ```vdisk123:conf:base```.  
It will return a BaseConfig and validate it. An error will be returned if a connection error occurred, the Base Config could not be found or it could be found but was invalid. The BaseConfig will always be defined if no error is returned.

### Watch
The source implementation for etcd provides methodes to be able to get subconfigs as they are being updated. It uses the etcdv3 Watch API to listen for updates of the key of a subconfig.

Those methodes return a channel that first sends the current subconfig. From then it sends a new subconfig when the etcd Watch API send an update for the key.

e.g.:
```go
func WatchNBDConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan NBDConfig, error)
```
WatchNBDConfigETCD will return a channel that sends a NBDConfig instance right at the start, and each time it is updated following that. If the NBDConfig didn't exist yet at startup or any other error occurred while doing the initial fetching, this method will return an error instead.

Learn more about:

+ [how to configure the NBD Server][nbdServerConfig];
+ [how to configure the TLog Server][tlogServerConfig];

[nbdServerConfig]: nbd/config.md
[tlogServerConfig]: tlog/config.md
[etcd]: https://github.com/coreos/etcd
[orchestrator]: https://github.com/zero-os/0-orchestrator
[configGodoc]:  https://godoc.org/github.com/zero-os/0-Disk/config
[baseconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#BaseConfig
[nbdconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#NBDConfig
[tlogconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#TlogConfig
[slaveconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#SlaveConfig