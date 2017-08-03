# NBD Server Configuration

The [NBD Server][nbd] and its components are normally configured using an [etcd][configetcd] distributed key-value store. For 0-Disk development purposes, one can also use a [file-based][configfile]-based configuration. As you should however use the [etcd-based][configetcd] configuration, especially in production, we'll use it in all examples that follow.

See [the 0-Disk config overview page][configDoc] for a more high level overview.

For more information about how to configure the [NBD Server][nbd] using [etcd][etcd], check out [the etcd config docs][configetcd].

## NBDVdisksConfig

This config is used so that the [nbdserver][nbdserver] can list all available [vdisks][vdisks] to an NBD Client, as well as to validate all (potentially) used configurations immediately when starting up the [nbdserver][nbdserver].

> This config is **REQUIRED**. Failing to list all available vdisks you wish to use, will result in undefined behaviour.

The vdisks config's purpose is to list all available [vdisks][vdisk]. Available in this context means, all [vdisks][vdisk] which have at least all required subconfigs stored in the same [etcd][etcd] cluster as this vdisks config, and you wish to actually use at some point of the [NBD Server][nbd]'s lifetime. This is required due to the `NBD_OPT_LIST` option of the NBD Protocol, as described in the [NBD Protocol docs][nbdprotocol]. This option is used to list all [vdisks][vdisk], and without it, some clients (such as `qemu`), do not want to load a vdisk, even though you give its ID explicitly for every command you issue.

This config requires you to also give the unique id of the [nbdserver][nbdserver] (shared with any linked [tlogserver][tlogserver]), as this config can only be found using that id as the prefix for the config key.

This config is stored in etcd under the `<serverID>:nbdserver:conf:vdisks` key. An example of its YAML-formatted value could look as follows:

```yaml
vdisks: # required
  - ubuntu:latest # only 1 is required, however.
  - arch:1992     # you should list all vdisks you wish to use
```

It is only loaded at startup, and each time it is updated in the [etcd][etcd] cluster. A cached version is used each time a new [vdisk][vdisk] is mounted and  consequently the `NBD_OPT_LIST` NBD option is triggered.

> NOTE that this config cannot be specified when using a [file-based][configfile] configuration. The vdisk identifiers are instead implicitly inferred using the root keys of the YAML config. See the [file-based configuration docs][configfile] for more information about this, in case (and only in case) you are a 0-Disk developer using it for development purposes.

See the [vdisks config docs][nbdVdisksConfig] on the [0-Disk config overview page][configDoc] for more information.

## Vdisk-specific Configurations

For each [vdisk][vdisk] that is to be mounted by the [nbdserver][nbdserver], there _has_ to be 2 configs. A [VdiskStaticConfig][vdiskStaticConfig] and a [VdiskNBDConfig][vdiskNBDConfig]. The former contains all static properties of a vdisk, while the latter contains the references (identifiers) of any used cluster. Only the primary storage cluster is required.

> NOTE: for each mounted [vdisk][vdisk], a [VdiskStaticConfig][vdiskStaticConfig] and a [VdiskNBDConfig][vdiskNBDConfig] config is **REQUIRED**. Failing to give those (correctly) will prevent you from mounting a [vdisk][vdisk].

> NOTE: make sure to define a [metadata][metadata] storage cluster for any primary/[slave storage][slave] cluster which is used for storing a ([semi][semideduped])[deduped][deduped] [vdisk][vdisk].

Some features are only supported by the [boot][boot]- and [db][db]- [vdisk][vdisk] types:

+ the template storage cluster is optional, and can be given in case the vdisk is to be based upon a [template][template];
+ the [tlog][tlog]- and [slave][slave]- clusters can be given and help make the data [redundant][redundant].

[nbd]: nbd.md
[nbdprotocol]: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md

[etcd]: https://github.com/coreos/etcd

[configDoc]: /docs/config.md
[configetcd]: /docs/config.md#etcd
[configfile]: /docs/config.md#file
[nbdVdisksConfig]: /docs/config.md#NBDVdisksConfig
[vdiskStaticConfig]: /docs/config.md#VdiskStaticConfig
[vdiskNBDConfig]: /docs/config.md#VdiskNBDConfig
[storageClusterConfig]: /docs/config.md#StorageClusterConfig

[nbdserver]: /docs/nbd/nbd.md

[metadata]: glossary.md#metadata
[redundant]: glossary.md#redundant
[data]: glossary.md#data
[vdisk]: /docs/glossary.md#vdisk
[template]: /docs/glossary.md#template
[tlog]: /docs/glossary.md#tlog
[slave]: /docs/glossary.md#slave
[boot]: /docs/glossary.md#boot
[db]: /docs/glossary.md#db
[deduped]: /docs/glossary.md#deduped
[semideduped]: /docs/glossary.md#semideduped