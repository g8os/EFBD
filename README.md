# G8OS Block Storage [![Build Status](https://travis-ci.org/g8os/blockstor.svg?branch=master)](https://travis-ci.org/g8os/blockstor)

This repository implements the components required for supporting block storage in G8OS nodes.

The G8OS block storage components allow you to create and use block devices (vdisks) from within virtual machines hosted on a G8OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

All documentation is in the [/docs](docs/SUMMARY.md) directory.

Components:
* [NBD Server](nbdserver/readme.md)
  - A network block device server to expose the vdisks to virtual machines
  - Documented [here](docs/nbd/nbd.md)
* [TLOG Server](tlog/readme.md)
  - A transaction log server to record block changes
  - Documented [here](docs/tlog/tlog.md)
* [CopyVdisk CLI](cmd/copyvdisk/readme.md)
  - A CLI tool to copy the metadata of a deduped vdis
  - Documented [here](docs/copyvdisk/copyvdisk.md)

Make sure to have [Golang](https://golang.org/) version 1.8 or above installed.
