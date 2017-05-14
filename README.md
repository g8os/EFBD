# G8OS Block Storage [![Build Status](https://travis-ci.org/g8os/blockstor.svg?branch=master)](https://travis-ci.org/g8os/blockstor)

This repository implements the components required for supporting block storage in G8OS nodes.

The G8OS block storage components allow you to create and use block devices (vdisks) from within virtual machines hosted on a G8OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

Components:
* [NBD Server](nbdserver/)
  - A network block device server to expose the vdisks to virtual machines
* [Gonbdserver](gonbdserver/)
  - An NBD server written in Go, integrated within the [NBD Server](nbdserver/)
* [TLOG Server](tlog/)
  - A transaction log server to record block changes
* [g8stor](g8stor/)
  - A CLI tool suite to control g8os resources

Make sure to have [Golang](https://golang.org/) version 1.8 or above installed.


## More

All documentation is in the [`/docs`](./docs) directory, including a [table of contents](/docs/SUMMARY.md).

In [Getting Started with NBD Server](/docs/gettingstarted/gettingstarted.md) you find the recommended path to quickly get up and running.
