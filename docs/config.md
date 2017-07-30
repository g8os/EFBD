# 0-Disk Configuration

All 0-Disk services are configured using a collection of subconfigurations, always serialized in the YAML format, and written by the [0-orchestrator][orchestrator]. 0-Disk services use the configs in read-only mode, and will thus never write to it.

More technical details and internal information can be found in the [Godocs][configGodoc].

Each vdisk has 4 subconfigs: [BaseConfig][baseconf], [NBDConfig][nbdconf], [TlogConfig][tlogconf] and [SlaveConfig][slaveconf]. There is also one global config, [VdisksConfig][vdisksconf] containing the list of all vdisks, used by the [NBD Server][nbd]. Meaning that if you have 2 vdisks, you will have up to 9 config keys.

The data is split up in this way so that for the use cases of 0-Disk, most of the time only one subconfig is required. Should another subconfig be required as well, it can easily be fetched from a source and stored together with the formar subconfig.

* [NBD Server][nbd] uses:
  * [BaseConfig][baseconf]: required (some fields are optional);
  * [NBDConfig][nbdconf]: required (some fields are optional);
* [Tlog Server][tlogserver] uses:
  * [TlogConfig][tlogconf]: required (some fields are optional);
  * [SlaveConfig][slaveconf]: optional (use it to enable slave cluster usage);

Normally the configurations are stored in [etcd][etcd], and fetched from the used cluster. However for isolated dev purposes one can also use a file, you can read the [file section](#file) to learn more about that. Make sure to read the [etcd section](#etcd) of this documentation if you are writing the configuration for any 0-Disk service, especially when doing so for production purposes.

> _All_ 0-Disk services expose a `-config` flag.

This flag is optional and will by default assume you are using a file config `config.yml` stored in the working dir. However in reality you will almost always want to use configuration originating from [etcd][etcd] instead. To use [etcd][etcd] configuration you can pass one or multiple dialstrings to the `-config` flag instead. e.g. `-config 127.0.0.1:2379`, would fetch the config values of a single [etcd][etcd] server, while `-config 232.201.201.101:2379,230.100.10.50:2379` would fetch it from an [etcd][etcd] cluster. Both IPv4 and IPv6 are supported.

Where important, any [NBDConfig][nbdconf], [TlogConfig][tlogconf] and [SlaveConfig][slaveconf], currently in use (by an active vdisks), will be automatically updated whenever a new revision of such config is written by the [0-orchestrator][orchestrator]. This is automatically done using the [etcd Watch API][etcdwatch]. No `SIGHUP` signal is required, unless you are using a file-originated config, instead of an [etcd][etcd]-originated config.

## Sub Configs

### Base Config

Stores general [VDisk][VDisk] information:

* BlockSize: Size of a [block][block] on the [VDisk][VDisk];
* ReadOnly: Defines if [VDisk][VDisk] is readonly
* Size: [VDisk][VDisk] size in GiB
* Type: Type of [VDisk][VDisk] ([boot][boot], [db][db], [cache][cache], [tmp][tmp])

Example Config:

```yaml
blockSize: 4096 # should be a power of 2
readOnly: false # optional, false by default
size: 10 	# has to be greater then 0
type: db	# should be a valid VDisk type (boot, db, cache or tmp)
```

Used by the [NBD Server][nbdServerConfig].

See the [BaseConfig Godoc][baseconfigGodoc] for more information.

### NBD Config

Stores nbd storage related information:

* StorageCluster: defines the storage cluster used to store [vdisk][vdisk] ([meta][metadata])[data][data];
* TemplateStorageCluster: defines the storage cluster used to store [template data][template] of a [vdisk][vdisk];
* TemplateVdiskID: defines the identifier of a [nondeduped][nondeduped] [template][template] [vdisk][vdisk];
* TlogServerAddresses: Defines a list of one or multiple [tlogserver][tlogserver] addresses;

YAML example:

```yaml
templateVdiskID: testtemplate # optional, same as VDisk ID if not defined,
                              # only used for nondeduped vdisks
storageCluster: # required
  dataStorage:  # at least one server is required
    - address: 192.168.1.146:2000 # has to be a valid dial string
      db: 10 # optional, 0 by default
    - address: 192.123.123.1:2001
      db: 10
  metadataStorage: # not used for nondeduped vdisks, required for the others
    address: 192.168.1.146:2001
    db: 11
templateStorageCluster: # optional, use it to enable a template vdisk
                        # (only supported for db and boot vdisks)
  dataStorage:
    - address: 192.168.1.147:2000
      db: 10
tlogServerAddresses: # optional array of network addresses
                     # use it to enable tlogstorage for this vdisk
                     # (only supported for db and boot vdisks)
  - 192.168.58.123:2000
  - 192.168.58.125:2000
```

Used by the [NBD Server][nbdServerConfig].

See the [NBDConfig Godoc][nbdconfigGodoc] for more information.

### TLog Config

Stores tlog storage and other information used by the [Tlog Server][tlogserver]:

* StorageCluster: defines the storage cluster used to store the [transaction logs][tlog] (will be removed in milestone 7, and deprecated by the 0-stor info);
* SlaveSync: defines if tlog should use the slave syncer, if true, the [SlaveConfig][slaveconf] becomes required;

YAML example:

```yaml
storageCluster: # required
  dataStorage: # m+k servers are required
    - address: 192.168.1.1:1000
      db: 14 # optional, 0 by default
  metadataStorage:
    address: 192.168.1.1:1001
    db: 18
slaveSync: true # optional, false by default
```

Used by the [Tlog Server][tlogServerConfig].

See the [TlogConfig Godoc][tlogconfigGodoc] for more information.

### Slave Config

Stores [slave][slave] storage cluster information:

* StorageCluster: defines the [slave][slave] storage cluster, replicating the primary storage cluster;

YAML example:

```yaml
storageCluster: # required
  dataStorage:  # at least 1 server required
    - address: 192.168.2.149:1000
      db: 14 # optional, 0 by default
  metadataStorage: # not used for nondeduped vdisks, required for all others
    address: 192.168.2.146:1001
    db: 18
```

Used by the [NBD Server][nbdServerConfig].

See the [SlaveConfig Godoc][slaveconfigGodoc] for more information

### Vdisks Config

Required by the [NBD Server][nbd] to list all available [vdisks][vdisk]:

* Vdisks: a list of identifiers, representing all available (and thus configured) vdisks;

YAML example:

```yaml
vdisks: # required
  - ubuntu:latest # at least one vdisk is required
  - arch:1992
```

Used by the [NBD Server][nbdServerConfig].

See the [VdisksConf Godoc][vdisksConfGodoc] for more information

## etcd

[etcd][etcd] is a distributed reliable key-value store and uses the Raft consensus algorithm to manage a highly-available replicated log. Support for etcd is added to ba able to store the configurations in a distributed way.

![etcd config overview](/docs/assets/config_overview_etcd.png)

### Reading data

The source implementation for [etcd][etcd] provides methodes to read and watch subconfigs from an etcd cluster. 
In the etcd cluster it will look for the following subconfig keys:

* [BaseConfig][baseconf]: `<VdiskID>:zerodisk:conf:base`;
* [NBDConfig][nbdconf]: `<VdiskID>:zerodisk:conf:nbd`;
* [TlogConfig][tlogconf]: `<VdiskID>:zerodisk:conf:tlog`;
* [SlaveConfig][slaveconf]: `<VdiskID>:zerodisk:conf:slave`;
* [VdisksConfig][vdisksconf]: `zerodisk:conf:vdisks`;

The values stored in those keys are the subconfigs serialised in the YAML format. Please make sure that each config is in the valid format, and has all required fields. Failing to do so, and the mounted [vdisk][vdisk] might misbehave or not work at all. Make sure to check the logs generated by the relevant 0-Disk service for debugging purposes of such issues.

### Watch

The source implementation for [etcd][etcd] provides methodes to be able to get subconfigs as they are being updated. It uses the [etcdv3 Watch API][etcdwatch] to listen for updates of the key of a subconfig. Meaning that as soon as [0-orchestrator][orchestrator] writes a new version of an existing config, the 0-Disk service will immediately reload this config value and use it, in case it is in use by a mounted [vdisk][vdisk].

Read [the internal Godoc documentation][configGodoc] for more technical details.

## file

Configuration of 0-Disk services using a single config file is also supported. **This should never be used for production**, and is only really meant for use by developers working on the 0-Disk repository. Because of this, no guarantees are made about backwards compatibility of its format. If you're using 0-Disk services in an ecosystem of things, or in production, you should be realy using the [etcd][etcd]-based configuration. Please take a lookg at the [etcd section](#etcd) section for more information on how to use and enable it.

### Reading data

When opting for file-based configuration, you store all subconfigs in a single yaml file, structured per vdisk. All 0-Disk services assume by default that you have a `config.yml` file in your current working directory. This can however be changed by use of the `-config` flag, which each 0-Disk service support. Passing in `-config /home/john/zero.cfg`, will for example use the file living at `/home/john/zero.cfg`.

Here is a full exmaple of how such a config file might look like:

```yaml
mini:
  base:
    blockSize: 4096
    readOnly: false
    size: 10
    type: boot
  nbd:
    storageCluster:
      dataStorage:
        - address: prime:16380
      metadataStorage:
        address: prime:16380
    tlogServerAddresses:
      - 127.0.0.1:2001
  slave:
    storageCluster:
      dataStorage:
        - address: tarzan:16381
        - address: tarzan:16382
      metadataStorage:
        address: tarzan:16381
        db: 4
  tlog:
    storageCluster:
      dataStorage:
        - address: bumblebee:6379
        - address: bumblebee:6380
        - address: bumblebee:6381
        - address: bumblebee:6382
        - address: bumblebee:6383
        - address: bumblebee:6384
    slaveSync: true
data:
  base:
    blockSize: 4096
    readOnly: true
    size: 2
    type: db
  nbd:
    storageCluster:
      dataStorage:
        - address: library:16385
      metadataStorage:
        address: library:16385
```

Please see the subconfigs' sections above for more information about each subsection.

NOTE that the [VdisksConfig][vdisksconf] does not have to be explicitly specified when using the file-based config, and is instead implicitly inferred using the root keys of the YAML config.

### Watch

The source implementation for the file-based config also supports watching the [NBDConfig][nbdconf], [TlogConfig][tlogconf] and [SlaveConfig][slaveconf]. Reloading of a config file happens as soon as you trigger a `SIGHUP` signal AND a config is used by an active (mounted) [vdisk][vdisk].

Read [the internal Godoc documentation][configGodoc] for more technical details.

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
[VDisk]: glossary.md#vdisk
[boot]: glossary.md#boot
[db]: glossary.md#db
[cache]: glossary.md#cache
[tmp]: glossary.md#tmp
[template]: glossary.md#template
[tlog]: glossary.md#TLog
[tlogserver]: tlog/server.md
[slave]: glossary.md#slave
[nbd]: nbd/nbd.md
[nbdServerConfig]: nbd/config.md
[tlogServerConfig]: tlog/config.md
[etcd]: https://github.com/coreos/etcd
[etcdwatch]: https://coreos.com/etcd/docs/latest/learning/api.html#watch-api
[orchestrator]: https://github.com/zero-os/0-orchestrator
[configGodoc]:  https://godoc.org/github.com/zero-os/0-Disk/config
[baseconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#BaseConfig
[nbdconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#NBDConfig
[tlogconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#TlogConfig
[slaveconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#SlaveConfig
[vdisksConfGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#VdisksConfig

[baseconf]: #base-config
[nbdconf]: #nbd-config
[tlogconf]: #tlog-config
[slaveconf]: #slave-config
[vdisksconf]: #vdisks-config