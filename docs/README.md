# Zero-OS 0-Disk

Zero-OS 0-Disk is about the components that allow to create and use [block][block] devices ([vdisks][vdisk]) from within virtual machines hosted on a Zero-OS node.

![0-Disk overview](/docs/assets/zerodisk_overview.png)

A [vdisk][vdisk] can be [deduped][deduped], have various [block][block] sizes and depending on the underlying [storage (1)][storage] cluster, have different speed characteristics.

Following vdisks types are supported:

- **Boot**: [redundant][redundant] disk that can be initialized based on a boot image ([template](template)). Boot disks should only be used for the operating system and applications installed on the operating system. Performance of boot disks is not optimized because that defeats their purpose.
- **DB**: [redundant][redundant] disk optimized for performant [block][block] storage. DB disks should be used for running reliable high IO intensive workloads such as databases, key value stores, ...
- **Cache**: [persistent][persistent] non-[redundant][redundant] disk optimized for performant [block][block] storage. Cache disks should be used for high IO intensive workloads which don't need to be highly reliable.
- **Tmp**: non-[persistent][persistent] non-[redundant][redundant] disk optimized for performant [block][block] storage. Content in tmp disks is only available while mounted. Tmp disks should be used for swap and mounts in `/tmp`.

| [VDisk][vdisk] type | [Redundant][redundant] | [Persistent][persistent] | [Template][template] Support | [Rollback][rollback] | Performance Optimized |
| --------- | --------- | ---------- | ---------------- | -------- | --------------------- |
| Boot | yes | yes | yes | yes | no |
| DB | yes | yes | no | yes | yes |
| Cache | no | yes | no | no | yes |
| Tmp | no | no | no | no | yes |

Zero-OS block storage is implemented in the [zero-os/0-Disk](https://github.com/zero-os/0-Disk) repository on GitHub.

Components:
* [NBD Server](nbd/nbd.md): a network [block][block] device ([NBD][nbd]) server to expose the [vdisks][vdisk] to virtual machines
* [TLOG Server/Client](tlog/tlog.md): a transaction [log (3)][log] [server][tlogserver] and [client][tlogclient] to record [block][block] changes
* [zeroctl](zeroctl/zeroctl.md): a command line tool to manage and discover [vdisks][vdisk]

See the [Table of Contents](SUMMARY.md) for all documentation.

See the [Glossary](/docs/glossary.md) as a reference for terminology specific to this project.


[block]: /docs/glossary.md#block
[vdisk]: /docs/glossary.md#vdisk
[deduped]: /docs/glossary.md#deduped
[storage]: /docs/glossary.md#storage
[redundant]: /docs/glossary.md#redundant
[persistent]: /docs/glossary.md#persistent
[template]: /docs/glossary.md#template
[rollback]: /docs/glossary.md#rollback
[log]: /docs/glossary.md#log
[nbd]: /docs/glossary.md#nbd

[tlogclient]: /docs/tlog/client.md
[tlogserver]: /docs/tlog/server.md
