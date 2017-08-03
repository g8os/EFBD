# zeroctl configuration

When copying, deleting and [restoring][restore] [vdisks][vdisk], the same configuration is required as is used for configuring the [nbdserver][nbdserver].

These commands are normally configured using an [etcd][configetcd] distributed key-value store. The etcd cluster can be passed to any command that requires 0-Disk configuration, using the provided `-config` flag. This takes a comma seperated network address list of etcd servers to use.

Please read the general and very elobrate [0-Disk config docs][configDocs] for more information about the different sub configurations, how to configure them, what they contain and more.

Please read the [zeroctl docs][zeroctlDocs] for more information about each command and how to use it.

## Copy a Vdisk

When copying the vdisk, the command requires:

+ The [VdiskStaticConfig][vdiskStaticConfig] of the source [vdisk][vdisk], because we need to know the type;
+ The [VdiskNBDConfig][vdiskNBDConfig] of the source [vdisk][vdisk], as to know which primary storage cluster is used;
+ The [StorageClusterConfig][primaryStorageCluster] used by the source [vdisk][vdisk] as the primary storage cluster, by referencing its id in the [VdiskNBDConfig][vdiskNBDConfig];

Optionally if a target cluster id is given:

+ The [StorageClusterConfig][primaryStorageCluster] linked to the given target cluster id, and to be used to copy the [vdisk][vdisk] to.

## Delete a Vdisk

When deleting [vdisks][vdisk], the command requires for each given vdiskID:

+ The [VdiskStaticConfig][vdiskStaticConfig] of the source [vdisk][vdisk], because we need to know the type;
+ The [VdiskNBDConfig][vdiskNBDConfig] of the source [vdisk][vdisk], as to know which primary storage cluster is used;
+ The primary [StorageClusterConfig][StorageClusterConfig] used by the source [vdisk][vdisk], by referencing its id in the [VdiskNBDConfig][vdiskNBDConfig];

## Restore a Vdisk

When restoring a [vdisk][vdisk], the command requires:

+ The [VdiskStaticConfig][vdiskStaticConfig] of the source [vdisk][vdisk], because we need to know the type;
+ The [VdiskNBDConfig][vdiskNBDConfig] of the source [vdisk][vdisk], as to know which primary storage cluster is used;
+ The primary [StorageClusterConfig][StorageClusterConfig] used by the source [vdisk][vdisk], by referencing its id in the [VdiskNBDConfig][vdiskNBDConfig] (used to write the data to);
+ The [tlog][tlog] [StorageClusterConfig][StorageClusterConfig] used by the source [vdisk][vdisk]'s used [tlogservers][tlogserver], by referencing its id in the [VdiskTlogConfig][VdiskTlogConfig] (used to read the data from);


[restore]: /docs/glossary.md#restore
[tlog]: /docs/glossary.md#tlog
[vdisk]: /docs/glossary.md#vdisk
[nbdserver]: /docs/nbd/nbd.md
[configDocs]: /docs/config.md
[zeroctlDocs]: /docs/zeroctl/zeroctl.md
[nbdserver]: /docs/nbd/nbd.md
[tlogserver]: /docs/tlog/tlog.md

[VdiskStaticConfig]: /docs/config.md#VdiskStaticConfig
[VdiskNBDConfig]: /docs/config.md#VdiskNBDConfig
[VdiskTlogConfig]: /docs/config.md#VdiskTlogConfig
[StorageClusterConfig]: /docs/config.md#StorageClusterConfig

[configetcd]: /docs/config.md#etcd