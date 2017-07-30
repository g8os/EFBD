# NBD Server Configuration

The [NBD Server][nbd] and its components are normally configured using an [etcd][configetcd] distributed key-value store. For 0-Disk development purposes, one can also use a [file-based][configfile]-based configuration. As you should however use the [etcd-based][configetcd] configuration, especially in production, we'll use it in all examples that follow.

The [NBD Server][nbd] requires the [Vdisks Configuration](#vdisks-config), [Base Configuration](#base-config) and [NBD Configuration](#nbd-config). See [the 0-Disk config overview page][configDoc] for more elaborate information of each of these sub configurations and more.

For more information about how to configure the [NBD Server][nbd] using [etcd][etcd], check out [the etcd config docs][configetcd].

## Vdisks Config

Let's start with the only global configuration. That is to say, a configuration which is not bound to any vdisk in particular, and instead is used for all of them.

> This config is **REQUIRED**. Failing to list all available vdisks you wish to use, will result in undefined behaviour.

The vdisks config's sole purpose is to list all available [vdisks][vdisk]. Available in this context means, all [vdisks][vdisk] which have at least all required subconfigs stored in the same [etcd][etcd] cluster as this vdisks config, and you wish to actually use at some point of the [NBD Server][nbd]'s lifetime. This is required due to the `NBD_OPT_LIST` option of the NBD Protocol, as described in the [NBD Protocol docs][nbdprotocol]. This option is used to list all [vdisks][vdisk], and without it, some clients (such as `qemu`), do not want to load a vdisk, even though you give its ID explicitly for every command you issue.

This config is stored in etcd under the `zerodisk:conf:vdisks` key. An example of its YAML-formatted value could look as follows:

```yaml
vdisks: # required
  - ubuntu:latest # only 1 is required, however.
  - arch:1992     # you should list all vdisks you wish to use
```

Because the vdisks config can get quite big, it is only loaded at startup, and each time it is updated in the [etcd][etcd] cluster. A cached version is used each time a new [vdisk][vdisk] is mounted and  consequently the `NBD_OPT_LIST` NBD option is triggered.

> NOTE that this config cannot be specified when using a [file-based][configfile] configuration. The vdisk identifiers are instead implicitly inferred using the root keys of the YAML config. See the [file-based configuration docs][configfile] for more information about this, in case (and only in case) you are a 0-Disk developer using it for development purposes.

See the [vdisks config docs][vdisksconf] on the [0-Disk config overview page][configDoc] for more information.

See the [VdisksConfig Godoc][vdisksconfigGodoc] for more information about the internal workings and how it is implemented in Go.

## Base Config

The Base config defines all the basic information of a [vdisk][vdisk], hence its name. It is used to configure a [vdisk][vdisk] while mounting it. Once mounted this information is used as static information, meaning it will never be reloaded during a [vdisk][vdisk] session. If you would like to update this configuration, you'll have to remount the [vdisk][vdisk].

> This config is **REQUIRED**. A [vdisk][vdisk] which does not define this configuration (correctly), cannot be mounted! The `readOnly` field is the only optional field of this configuration and is `false` by default.

This config is stored in etcd under the `<vdiskid>:zerodisk:conf:base` key format. For a [vdisk][vdisk] named `foo`, this key would thus become `foo:zerodisk:conf:base`. An example of its YAML-formatted value could look as follows:

```yaml
blockSize: 4096 # should be a power of 2
readOnly: false # optional, false by default
size: 1 	# has to be greater then 0
type: boot	# should be a valid VDisk type (boot, db, cache or tmp)
```

See the [base config docs][baseconf] on the [0-Disk config overview page][configDoc] for more information.

See the [BaseConfig Godoc][baseconfigGodoc] for more information about the internal workings and how it is implemented in Go.

## NBD Config

The NBD config is the last subconfig used by the [NBD Server][nbd]. It contains all the information required to store the ([meta][metadata])[data][data] of a [vdisk][vdisk] and also optional information which helps it to keep al the stored ([meta][metadata])[data][data] [redundant][redundant] by logging all transactions optionally it to the [TLog Server][tlogserver].

> This config is **REQUIRED**. A [vdisk][vdisk] which does not define this configuration (correctly), cannot be mounted! Most fields however are optional, and can be given to enable certain features. If a [vdisk][vdisk] doesn't support a certain feature, those fields are ignored that would otherwise activate this feature.

This config is stored in etcd under the `<vdiskid>:zerodisk:conf:nbd` key format. For a [vdisk][vdisk] named `foo`, this key would thus become `foo:zerodisk:conf:nbd`. An example of its YAML-formatted value could look as follows:

```yaml
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
templateVdiskID: testtemplate # optional, same as VDisk ID if not defined,
                              # only used for nondeduped vdisks
tlogServerAddresses: # optional array of network addresses
                     # use it to enable the logging of transactions for this vdisk
                     # (only supported for db and boot vdisks)
  - 192.168.58.123:2000 # when given, only 1 dialstring is required,
  - 192.168.58.125:2000 # but more can be optionally given and serve as a backup.
```

See the [nbd config docs][nbdconf] on the [0-Disk config overview page][configDoc] for more information.

See the [NBDConfig Godoc][nbdconfigGodoc] for more information about the internal workings and how it is implemented in Go.


[nbd]: nbd.md
[nbdprotocol]: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md

[etcd]: https://github.com/coreos/etcd
[etcdwatch]: https://coreos.com/etcd/docs/latest/learning/api.html#watch-api

[configDoc]: /docs/config.md
[configetcd]: /docs/config.md#etcd
[configfile]: /docs/config.md#file
[vdisksconf]: /docs/config.md#vdisks-config
[baseconf]: /docs/config.md#base-config
[nbdconf]: /docs/config.md#nbd-config
[tlogconf]: /docs/config.md#tlog-config
[slaveconf]: /docs/config.md#slave-config
[tlogserver]: /docs/tlog/server.md

[metadata]: glossary.md#metadata
[redundant]: glossary.md#redundant
[data]: glossary.md#data

[configGodoc]:  https://godoc.org/github.com/zero-os/0-Disk/config
[baseconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#BaseConfig
[nbdconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#NBDConfig
[tlogconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#TlogConfig
[slaveconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#SlaveConfig
[vdisksconfigGodoc]: https://godoc.org/github.com/zero-os/0-Disk/config#VdisksConfig


[vdisk]: /docs/glossary.md#vdisk