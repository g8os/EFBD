# 0-Disk Configuration

All 0-Disk services are configured using a YAML config source, written by the [0-orchestrator][orchestrator].

More technical details can be found in the [Godocs][configGodoc]

The config data is stored in 4 subconfigs: BaseConfig, NBDConfig, TlogConfig and SlaveConfig (slave config information). The information is stored (in sources) and processed internally in YAML format.

* BaseConfig: stores general vdisk information
	* BlockSize
	* ReadOnly
	* Size
	* Type: Type of VDisk (Boot, DB, Cache, tmp)
	* TlogRPC: defines the address of the tlogserver 
* NBDConfig: stores NBD server information
	* StorageCluster
	* TemplateStorageCluster
	* TemplateVdiskID
* TlogConfig: stores tlog server information
	* TlogStorageCluster
	* SlaveSync
* SlaveConfig: stores slave storage cluster information
	* SlaveStorageCluster

Each subconfig has a constructor, ToBytes and a Validate methode.
The constructor will unmarshal a subconfig from on a YAML formatted slice of byte, validates the subconfig and returns an error if invalid.
ToBytes will marchal the subconfig to a YAML slice of bytes and return it.
Validate will validate the current subconfig. It is however only recommended to be used before setting a subconfig to a source in that source implementation for the configs.

The data is split up in this way so that for the use cases of 0-Disk, most of the time only the BaseConfig and/or one of the other subconfigs would be required. Should another subconfig be required, it can easily be fetched from a source and stored together with the other subconfigs.

* NBD Server uses: BaseConfig, NBDConfig, SlaveConfig
* Tlog Server (server.vdisk) uses: TlogConfig

The source implementations of the config (etcd) allows for data being read from the source and include a watch/update feature that returns an updated subconfig as it is updated by the 0-orchestrator. This feature is not implemented for BaseConfig as that data should be quite static.

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

When the values are read from etcd, the subconfig will be created with the New<Sub>Config constructor, which will also check if the provided byte slice is a valid subconfig.

e.g.:
```go
func ReadBaseConfigETCD(vdiskID string, endpoints []string) (*BaseConfig, error)
```
ReadBaseConfigETCD will read a Baseconfig looking on an etcd cluster from provided endpoint(s) that is parth of the cluster. It will then look for the following key ```<VdiskID>:conf:base``` generated with the provided vdiskID. If the vdiskID is ```vdisk123``` then it will look for the following key: ```vdisk123:conf:base```.  
It will return a BaseConfig if it is validated. If a connection error occured or invalid Baseconfig config is found, it will return an error.

### Watch
The source implementation for etcd provides methodes to be able to get subconfigs as they are being updated. It uses the etcdv3 Watch API to listen for updates of the key of a subconfig.

Those methodes return a channel that first sends the current subconfig. From then it sends a new subconfig when the etcd Watch API send an update for the key.

The goroutine listening for the Watch API is closed when the cancelFunc from the provided context is called or any other reason the context would send a context.Done signal. The update channel will not be closed.

e.g.:
```go
func WatchNBDConfigETCD(ctx context.Context, vdiskID string, endpoints []string) (<-chan NBDConfig, error)
```
WatchNBDConfigETCD will return a channel that sends NBDConfig's. As seen as this function is called, it will fetch and validate the NBDConfig on the etcd cluster. If successful it will return the channel and send the current NBDConfig on etcd. If not and error is returned.
WatchNBDConfigETCD also launches a Goroutine and using the etcd Watch API it will send an updated NBDConfig to the returned channel when they are received from the Watch API and validated. If the updated value on etcd is not valid, no new NBDConfig will be send to the channel and the error will be logged.


Learn more about:

+ [how to configure the NBD Server](nbd/config.md);
+ [how to configure the TLog Server](tlog/config.md);

[orchestrator]: https://github.com/zero-os/0-orchestrator
[configGodoc]:  https://godoc.org/github.com/zero-os/0-Disk/config
[etcd]: https://github.com/coreos/etcd