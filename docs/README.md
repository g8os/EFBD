# G8OS Block Storage

G8OS Block Storage is about the components that allow to create and use block devices (vdisks) from within virtual machines hosted on a G8OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

G8OS Block Storage is implemented in the [zero-os/0-Disk](https://github.com/zero-os/0-Disk) repository on GitHub.

Components:
* [NBD Server](nbd/nbd.md): A network block device server to expose the vdisks to virtual machines
* [TLOG Server](tlog/tlog.md): A transaction log server to record block changes
* [Command Line Tool](commandlinetool/commandlinetool.md): A command line tool suite to manage vdisks

See the [Table of Contents](SUMMARY.md) for all documentation.
