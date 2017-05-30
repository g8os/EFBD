# Zero-OS 0-Disk

Zero-OS 0-Disk is about the components that allow to create and use block devices (vdisks) from within virtual machines hosted on a Zero-OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

Following vdisks types are supported:

- **Boot**: redundant disk that can be initialized based on a boot image (template). Boot disks should only be used for the operating system and applications installed on the operating system. Performance of boot disks is not optimized because that defeats their purpose.
- **DB**: redundant disk optimized for performant block storage. DB disks should be used for running reliable high IO intensive workloads such as databases, key value stores, ...
- **Cache**: persistent non-redundant disk optimized for performant block storage. Cache disks should be used for high IO intensive workloads which don't need to be highly reliable.
- **Tmp**: non-persistent non-redundant disk optimized for performant block storage. Content in tmp disks is only available while mounted. Tmp disks should be used for swap and mounts in `/tmp`.

| Disk type | Redundant | Persistent | Template Support | Rollback | Performance Optimized |
| --------- | --------- | ---------- | ---------------- | -------- | --------------------- |
| Boot | yes | yes | yes | yes | no |
| DB | yes | yes | no | yes | yes |
| Cache | no | yes | no | no | yes |
| Tmp | no | no | no | no | yes |

Zero-OS block storage is implemented in the [zero-os/0-Disk](https://github.com/zero-os/0-Disk) repository on GitHub.

Components:
* [NBD Server](nbd/nbd.md): A network block device server to expose the vdisks to virtual machines
* [TLOG Server](tlog/tlog.md): A transaction log server to record block changes
* [Command Line Tool](commandlinetool/commandlinetool.md): A command line tool suite to manage vdisks

See the [Table of Contents](SUMMARY.md) for all documentation.
