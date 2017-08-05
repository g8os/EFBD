# 0-Disk Configuration

All 0-Disk services are configured using a collection of subconfigurations, always serialized in the YAML format, and written by the [0-orchestrator][orchestrator]. 0-Disk services use the configs in read-only mode, and will thus never write to it.

More technical details and internal information can be found in the [Godocs][configGodoc].

What follows is an example to showcase the relationships between the different sub configurations and how they form to be a [vdisk][vdisk]. Check out the [etcd section](#etcd) for more information about the specific key formats.

![subconfig relationships](/docs/assets/config_relations.png)

Each vdisk has 2 required configurations, [VdiskStaticConfig](#VdiskStaticConfig) and [VdiskNBDConfig](#VdiskNBDConfig). On top of that there is an optional [VdiskTlogConf](#VdiskTlogConfig) for those vdisks which require [TLog][tlog] support. Each vdisks reference the storage clusters it depends upon using their unique identifiers. The actual configurations for these storage clusters are stored inside [StorageClusterConfig](#StorageClusterConfig) ([ARDB][ardb]) and [TlogClusterConfig](#TlogClusterConfig) ([TLog][tlog]).

There is one last type of configuration, [NBDVdisksConfig](#NBDVdisksConfig), which contains the identifiers of all vdisks used by a [nbdserver][nbdserver]+[tlogserver][tlogserver] group.


> NOTE: both the [nbdserver][nbdserver] and [tlogserver][tlogserver] also require you to specify the shared server id in using the `-id` flag. This is the same id as the one you used to list all vdisks in the [NBDVdisksConfig](#NBDVdisksConfig). By default it is assumed this id is `default`, but this should probably always be overwritten by the 0-Orchestrator.

The data is split up in this way so that for the use cases of 0-Disk, most of the time only one (sub)config is required. Should another (sub)config be required as well, it can easily be fetched from a source (e.g. [etcd](#etcd)) and stored together with the former (sub)config.

On top of that it is designed to avoid duplication of data as much as possible. Should a specific [storage cluster](#StorageClusterConfig) be used by multiple [vdisks][vdisk], it would only have to be defined once, and can be referenced by all these vdisks simply using its identifier. This has the additional advantage that if this cluster should have to be updated due to one or multiple unreachable servers, it will be fixed for all these [vdisks][vdisk] at once.

Who uses what:

* [NBD Server][nbdserver] uses:
  * [NBDVdisksConfig](#NBDVdisksConfig): required;
  * for each [VDisk][vdisk]:
    * [VdiskStaticConfig](#VdiskStaticConfig): required (some fields are optional);
    * [VdiskNBDConfig](#VdiskNBDConfig): required (some fields are optional);
  * for each referenced storage cluster:
    * [StorageClusterConfig](#StorageClusterConfig):
      * required for [primary storage (1)][storage] (some fields are not used);
      * optional for [template][template] and [slave storage][slave] (some fields are optional);
  * for each referenced tlog cluster:
    * [TlogClusterConfig](#TlogClusterConfig): optional;
* [Tlog Server][tlogserver] (which itself is optional) uses:
  * [NBDVdisksConfig](#NBDVdisksConfig): required;
  * for each [VDisk][vdisk]:
    * [VdiskTlogConfig](#VdiskTlogConfig): required (some fields are optional);
  * for each referenced storage cluster:
    * [StorageClusterConfig](#StorageClusterConfig):
      * required for [tlog storage (1)][storage] (some fields are not used);
      * optional for [slave storage][slave] (some fields are optional);
* [zeroctl][zeroctl] (copy/delete/restore) uses:
  * for most [VDisk][vdisk] operations:
    * [VdiskStaticConfig](#VdiskStaticConfig): required (some fields are optional);
    * [VdiskNBDConfig](#VdiskNBDConfig): required (some fields are optional);
  * for each referenced storage cluster:
    * [StorageClusterConfig](#StorageClusterConfig):

## Config Source

Normally the configurations are stored in [etcd][etcd], and fetched from the used cluster. However for isolated dev purposes one can also use a file, you can read the [file section](#file) to learn more about that. Make sure to read the [etcd section](#etcd) of this documentation if you are writing the configuration for any 0-Disk service, especially when doing so for production purposes.

> _All_ 0-Disk services expose a `-config` flag.

This flag is optional and will by default assume you are using a file config `config.yml` stored in the working dir. However in reality you will almost always want to use configuration originating from [etcd][etcd] instead. To use [etcd configuration](#etcd) you can pass one or multiple dialstrings to the `-config` flag instead. e.g. `-config 127.0.0.1:2379`, would fetch the config values of a single [etcd][etcd] server, while `-config 232.201.201.101:2379,230.100.10.50:2379` would fetch it from an [etcd][etcd] cluster. Both IPv4 and IPv6 are supported.

Where important, any subconfig which supports hot reloading and is currently in use (by any active vdisks), will be automatically updated whenever a new revision of such config is written by the [0-orchestrator][orchestrator]. This is automatically done using the [etcd Watch API][etcdwatch]. No `SIGHUP` signal is required to trigger the reloading of any watched config, unless you are using a [file-originated config](#file) instead of an [etcd-originated config](#etcd).

## Config Hot Reloading

As mentioned before, some configs support [hot reloading][hotreload]. That is to say, they allow a controller to update the configuration and have it be used by any active [vdisks][vdisk], without having to reboot such [vdisks][vdisk]. Configs supporting hot reloading are: [NBDVdisksConfig](#NBDVdisksConfig), [VdiskNBDConfig](#VdiskNBDConfig), [VdiskTlogConfig](#VdiskTlogConfig), [StorageClusterConfig](#StorageClusterConfig) and [TlogClusterConfig](#TlogClusterConfig).

Because of self-healing for example it is required that a primary storage cluster can be updated, or to be able to start using a slave storage cluster as the primary storage cluster. This is however just one example of many where hot reloading is a very useful feature to have.

![hotreload simple](/docs/assets/config_hotreload_simple.png)

For a simple configuration such as the [NBDVdisksConfig](#NBDVdisksConfig), its hot reload use case is very simple. As can be seen above, there is simply a watcher which is mainly used to watch for any updates, and dispatch any updates that do come in to the user/creator of that watcher.

![hotreload composition](/docs/assets/config_hotreload_composition.png)

For composed configurations the situation is however a bit more complex. Here we have a composed configuration, called `NBDstorage`, which collects the primary-, template- and slave storage cluster, as well as the tlog cluster. All subconfigs (and thus subwatchers) are optional except for the primary storage cluster.

In this example the final configuration is updated in 2 different scenarios:

+ The vdisk's NBD config is updated. In this case, the referenced ids are checked against the once already used (and watched). If a referenced id is different, it is recreated. If the update makes an id reference no longer exist, the previous used watcher is deleted. If no id reference is different, there is no update send to the user;
+ A referenced cluster is update, if the new cluster is different and valid, it is send in updated form to the user (of that config);

## Sub Configs

<a id="VdiskStaticConfig"></a>
### VdiskStaticConfig

Stores static [VDisk][vdisk] information:

* BlockSize: Size of a [block][block] on the [VDisk][VDisk];
* ReadOnly: Defines if [VDisk][VDisk] is readonly;
* Size: [VDisk][VDisk] size in GiB;
* Type: Type of [VDisk][VDisk] ([boot][boot], [db][db], [cache][cache], [tmp][tmp]);
* TemplateVdiskID: ID of [template vdisk][template], only used by [nondeduped vdisks][nondeduped];

Example Config:

```yaml
blockSize: 4096 # should be a power of 2
readOnly: false # optional, false by default
size: 10 	# has to be greater then 0
type: db	# should be a valid VDisk type (boot, db, cache or tmp)
templateVdiskID: foo	# optional, equal to the vdiskID if not given
                      # (used for nondeduped vdisks only)
```

Used by the [NBD Server][nbdServerConfig].

See the [VdiskStaticConfig Godoc][VdiskStaticConfigGodoc] for more information.

<a id="VdiskNBDConfig"></a>
### VdiskNBDConfig

Stores [storage(1)][storage]/[tlog][tlog] cluster references for a [vdisk][vdisk]:

* StorageClusterID: identifier of primary [storage][storage] cluster;
* Properties supported only by [boot][boot]- and [db][db]- [vdisks][vdisk]:
  * TemplateStorageClusterID: identifier of [template storage][template] cluster;
  * SlaveStorageClusterID: identifier of [slave storage][slave] cluster, should only ever be used in combination with a [tlog server][tlogserver] cluster;
  * TlogServerClusterID: Tidentifier of [tlog server][tlogserver] cluster, when given it enabled tlog storage;

Example Config:

```yaml
storageClusterID: dasdaf # required, id of primary storage cluster
templateStorageClusterID: dsadad # optional, id of template storage cluster
slaveStorageClusterID: fdfsfsfs # optional, id of slave storage cluster
                                # (only supported for tlog-supported vdisks: db and boot)
                                # (only used by tlog-supported vdisks which have specified a tlogServerClusterID)
tlogServerClusterID: db	# optional, id of a tlog server cluster
                        # enables the tlog feature when given,
                        # for those vdisks that support it (db and boot)
```

Used by the [NBD Server][nbdServerConfig].

See the [VdiskNBDConfig Godoc][VdiskNBDConfigGodoc] for more information.

<a id="VdiskTlogConfig"></a>
### VdiskTlogConfig

Stores [storage(1)][storage] cluster references for a ([boot][boot]- or [db][db]-) [vdisk][vdisk]:

* StorageClusterID: identifier of [tlog storage][tlog] cluster;
* SlaveStorageClusterID: identifier of [slave storage][slave] cluster (the same id as given to the [VdiskNBDConfig](#VdiskNBDConfig));

Example Config:

```yaml
storageClusterID: dasdaf # required, id of primary storage cluster
                         # the cluster linked, requires m+k
                         # data storage servers to be specified
slaveStorageClusterID: fdfsfsfs # optional, id of slave storage cluster
```

Used by the [TLog Server][tlogServerConfig].

See the [VdiskTlogConfig Godoc][VdiskTlogConfigGodoc] for more information.

<a id="StorageClusterConfig"></a>
### StorageClusterConfig

Stores [storage(1)][storage] cluster information, referenced by one or multiple [vdisks][vdisk]:

* dataStorage: data ([ARDB][ardb]) servers used to store vdisk and tlog data;
* metaDataStorage: a single ([ARDB][ardb]) server used to store vdisk [metadata][metadata] (not used by [nondeduped vdisks][nondeduped]);

Example Config:

```yaml
dataStorage: # required
  dataStorage:  # at least one server is required
    - address: 192.168.1.146:2000 # has to be a valid dial string
      db: 10 # optional, 0 by default
    - address: 192.123.123.1:2001
      db: 10
  metadataStorage: # not used for nondeduped vdisks and template storage,
                   # required for all other purposes
    address: 192.168.1.146:2001
    db: 11
```

Used by both the [NBD Server][nbdServerConfig] and the [TLog Server][tlogServerConfig].

See the [StorageClusterConfig Godoc][StorageClusterConfigGodoc] for more information.

<a id="TlogClusterConfig"></a>
### TlogClusterConfig

Stores [tlog][tlog] cluster information, referenced by one or multiple [vdisks][vdisk] (on a single [nbdserver][nbdserver]):

* servers: one dial string per tlog server to use (at least one is required);

Example Config:

```yaml
servers: # required
  - localhost:20031 # at least one is required
  - localhost:20042 # the rest serves as a backup
  - localhost:20321 # for if the one used fails
```

Used by the [NBD Server][nbdServerConfig].

See the [TlogClusterConfig Godoc][TlogClusterConfigGodoc] for more information.

<a id="NBDVdisksConfig"></a>
### NBDVDisksConfig

Stores a list of identifiers of all available [vdisks][vdisk]:

* vdisks: an array of identifiers of available vdisks;

Example Config:

```yaml
vdisks: # required
  - foo # at least one is required
  - bar # more is optional
```

Used by both the [NBD Server][nbdServerConfig] and the [TLog Server][tlogServerConfig].
Both ([nbdserver][nbdserver] and [tlogserver][tlogserver]) use it for validation of all used vdisks while starting the servers,
while the [nbdserver][nbdserver] also uses it for the export controller, which is used by the underlying gonbdserver (which implements a NBD protocol server).

See the [NBDVDisksConfig Godoc][NBDVDisksConfigGodoc] for more information.

## etcd

[etcd][etcd] is a distributed reliable key-value store and uses the Raft consensus algorithm to manage a highly-available replicated log. Support for etcd is added to ba able to store the configurations in a distributed way.

### Reading data

The source implementation for [etcd][etcd] provides methodes to read and watch subconfigs from an etcd cluster.
In the etcd cluster it will look for the following subconfig keys:


* [VdiskStaticConfig](#VdiskStaticConfig): `<VdiskID>:vdisk:conf:static`;
* [VdiskNBDConfig](#VdiskNBDConfig): `<vdiskID>:vdisk:conf:storage:nbd`;
* [VdiskTlogConfig](#VdiskTlogConfig): `<vdiskID>:vdisk:conf:storage:tlog`;
* [StorageClusterConfig](#StorageClusterConfig): `<clusterID>:cluster:conf:storage`;
* [TlogClusterConfig](#TlogClusterConfig): `<clusterID>:cluster:conf:tlog`;
* [NBDVdisksConfig](#NBDVdisksConfig): `<serverID>:nbdserver:conf:vdisks`;

The values stored in those keys are the subconfigs serialised in the YAML format. Please make sure that each config is in the valid format, and has all required fields. Failing to do so, and the config will not be reloaded. Or in case this happens at startup, the vdisk might not be mounted at all. Make sure to check the logs generated by the relevant 0-Disk service for debugging purposes of such issues. In case this happens during a hot reloading action, the [0-orchestrator][orchestrator] will be notified.

### Watch

The source implementation for [etcd][etcd] provides methodes to be able to get subconfigs as they are being updated. It uses the [etcdv3 Watch API][etcdwatch] to listen for updates of the key of a subconfig. Meaning that as soon as [0-orchestrator][orchestrator] writes a new version of an existing config, the 0-Disk service will immediately reload this config value and use it, in case it is in use by a mounted [vdisk][vdisk].

Invalid configurations are never dispatched to the config-user, and any such situation is logged to the [0-orchestrator][orchestrator].

Read [the internal Godoc documentation][configGodoc] for more technical details.

## file

Configuration of 0-Disk services using a single config file is also supported. **This should never be used for production**, and is only really meant for use by developers working on the 0-Disk repository. Because of this, no guarantees are made about backwards compatibility of its format. If you're using 0-Disk services in an ecosystem of things, or in production, you should be realy using the [etcd][etcd]-based configuration. Please take a lookg at the [etcd section](#etcd) section for more information on how to use and enable it.

### Reading data

When opting for file-based configuration, you store all subconfigs in a single yaml file, structured per vdisk. All 0-Disk services assume by default that you have a `config.yml` file in your current working directory. This can however be changed by use of the `-config` flag, which each 0-Disk service support. Passing in `-config /home/john/zero.cfg`, will for example use the file living at `/home/john/zero.cfg`.

Here is a full exmaple of how such a config file might look like:

```yaml
storageClusters:
  mycluster:
    dataStorage:
      - address: localhost:16379
      - address: localhost:16380
      - address: localhost:16381
    metadataStorage:
      address: localhost:16379
      db: 2
  mastercluster:
    dataStorage:
      - address: localhost:10122
      - address: localhost:10123
      - address: localhost:10124
  slavecluster:
    dataStorage:
      - address: localhost:20122
      - address: localhost:20123
      - address: localhost:20124
    metadataStorage:
      address: localhost:16312
      db: 1
  tlogcluster:
    dataStorage:
      - address: localhost:10001
      - address: localhost:10002
      - address: localhost:10003
      - address: localhost:10004
      - address: localhost:10005
tlogClusters:
  main:
    servers:
      - localhost:20031
      - localhost:20032
vdisks:
  data:
    type: db
    blockSize: 512
    size: 10
    nbd:
      storageClusterID: mycluster
      tlogServerClusterID: main
    tlog:
      storageClusterID: tlogcluster
  mini:
    type: boot
    blockSize: 4096
    size: 1
    nbd:
      storageClusterID: mycluster
      templateStorageClusterID: mastercluster
      slaveStorageClusterID: slavecluster
      tlogServerClusterID: main
    tlog:
      storageClusterID: tlogcluster
      slaveStorageClusterID: slavecluster
```

Please see the subconfigs' sections above for more information about each subsection.

NOTE that the [NBDVdisksConfig](#NBDVdisksConfig) does not have to be explicitly specified when using the file-based config, and is instead implicitly inferred using the root keys of the YAML config.

### Watch

The source implementation for the file-based config also supports watching subconfigs. Reloading of a config file happens as soon as you trigger a `SIGHUP` signal AND a config is used by an active (mounted) [vdisk][vdisk].

Read [the internal Godoc documentation][configGodoc] for more technical details.

### Converting to an etcd config

In case you would ever want to test during development on an etcd cluster,
you can instantly import your file-based configuration into a(n) (local) etcd cluster.

You can do so using the [import_file_into_etcd_config tool](/tools/import_file_into_etcd_config):

```shell
$ import_file_into_etcd_config \
  -id myserver -path ./config.yml \
  127.0.0.1:2379
```

This command will import the yaml config located at `./config.yml`
into the etcd server listening for incoming requests at `127.0.0.1:2379`.

Note that just as file based configs are just meant for quick development and testing, so is this tool meant for development purposes only.

Please read [the import_file_into_etcd_config tool README](/tools/import_file_into_etcd_config/README.md) for more information about this tool, how to use it, what each flag does exactly, and more.

## Where to go from here

Learn more about:

* [How to configure the NBD Server.][nbdServerConfig];
* [How to configure the TLog Server.][tlogServerConfig];

[ardb]: /nbd/ardb/ardb.go
[block]: glossary.md#block
[redispool]: /tlog/redispool.go
[metadata]: glossary.md#metadata
[nondeduped]: glossary.md#nondeduped
[data]: glossary.md#data
[zeroctl]: zeroctl/zeroctl.md
[VDisk]: glossary.md#vdisk
[boot]: glossary.md#boot
[db]: glossary.md#db
[cache]: glossary.md#cache
[tmp]: glossary.md#tmp
[template]: glossary.md#template
[hotreload]: glossary.md#hotreload
[storage]: glossary.md#storage
[tlog]: glossary.md#TLog
[nbdserver]: nbd/nbd.md
[tlogserver]: tlog/server.md
[slave]: glossary.md#slave
[nbd]: nbd/nbd.md
[nbdServerConfig]: nbd/config.md
[tlogServerConfig]: tlog/config.md
[etcd]: https://github.com/coreos/etcd
[etcdwatch]: https://coreos.com/etcd/docs/latest/learning/api.html#watch-api
[orchestrator]: https://github.com/zero-os/0-orchestrator
[configGodoc]:  https://godoc.org/github.com/zero-os/0-Disk/config
[VdiskStaticConfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#VdiskStaticConfig
[VdiskNBDConfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#VdiskNBDConfig
[VdiskTlogConfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#VdiskTlogConfig
[StorageClusterConfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#StorageClusterConfig
[TlogClusterConfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#TlogClusterConfig