# Tlog Server Configuration

The [Tlog Server][tlogserver] and its components are normally configured using an [etcd][configetcd] distributed key-value store. For 0-Disk development purposes, one can also use a [file-based][configfile]-based configuration. As you should however use the [etcd-based][configetcd] configuration, especially in production, we'll use it in all examples that follow.

Please read the general and very elobrate [0-Disk config docs][configDoc] for more information about the different sub configurations, how to configure them, what they contain and more.

## Vdisk-specific Configurations

For each [vdisk][vdisk] that is to be mounted by the [tlogserver][tlogserver], there _has_ to be 2 configs. A [VdiskStaticConfig][vdiskStaticConfig] and a [VdiskTlogConfig][VdiskTlogConfig]. The former contains all static properties of a vdisk, while the latter contains the references (identifiers) of any used cluster. Only the tlog storage cluster is required.

> NOTE: for each mounted [vdisk][vdisk], a [VdiskStaticConfig][vdiskStaticConfig] and a [VdiskTlogConfig][VdiskTlogConfig] config is **REQUIRED**. Failing to give those (correctly) will prevent you from using a [tlogserver][tlogserver] for a [vdisk][vdisk].

A [slave][slave] [StorageClusterConfig][StorageClusterConfig] can be given, but this is not required at the moment.


[nbd]: nbd.md
[nbdprotocol]: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md

[etcd]: https://github.com/coreos/etcd

[configDoc]: /docs/config.md
[configetcd]: /docs/config.md#etcd
[configfile]: /docs/config.md#file
[nbdVdisksConfig]: /docs/config.md#NBDVdisksConfig
[vdiskStaticConfig]: /docs/config.md#VdiskStaticConfig
[vdiskTlogConfig]: /docs/config.md#VdiskTlogConfig
[storageClusterConfig]: /docs/config.md#StorageClusterConfig

[nbdserver]: /docs/nbd/nbd.md
[tlogserver]: /docs/tlogserver/mainbd.md

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