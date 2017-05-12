# G8OS Block Storage [![Build Status](https://travis-ci.org/g8os/blockstor.svg?branch=master)](https://travis-ci.org/g8os/blockstor)

This repository implements the components required for supporting block storage in G8OS nodes.

The G8OS block storage components allow you to create and use block devices (vdisks) from within virtual machines hosted on a G8OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

Components:
* [NBD Server](nbdserver/)
  - A network block device server to expose the vdisks to virtual machines
* [TLOG Server](tlog/)
  - A transaction log server to record block changes
* [Copyvdisk](cmd/copyvdisk/)
  - A CLI tool to copy the metadata of a deduped vdis

Make sure to have [Golang](https://golang.org/) version 1.8 or above installed.


## More

All documentation is in the [`/docs`](./docs) directory, including a [table of contents](/docs/SUMMARY.md).

In [Getting Started with NBD Server](/docs/gettingstarted/gettingstarted.md) you find the recommended path to quickly get up and running.
