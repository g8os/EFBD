# G8OS Block Storage [![Build Status](https://travis-ci.org/g8os/blockstor.svg?branch=master)](https://travis-ci.org/g8os/blockstor)

The G8OS block storage allows to create and use block devices (vdisks) from within virtual machines hosted on a G8OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

Make sure to have Golang version 1.8 or above installed!

Components:
* [NBD Server](nbdserver/readme.md)
    A Network Block Device server to expose the vdisks to virtual machines
* [TLOG Server](tlog/tlogserver/README.md)
    A Transaction Log server to record block changes
* [CopyVdisk CLI](cmd/copyvdisk/readme.md)
    A CLI tool to copy the metadata of a deduped vdisk

All documentation is in the [/docs](docs/SUMMARY.md) directory.
