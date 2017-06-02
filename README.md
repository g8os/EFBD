# Zero-OS 0-Disk [![Build Status](https://travis-ci.org/zero-os/0-Disk.svg?branch=master)](https://travis-ci.org/zero-os/0-Disk)

This repository implements the components required for supporting block storage in Zero-OS nodes.

The Zero-OS block storage components allow you to create and use block devices (vdisks) from within virtual machines hosted on a Zero-OS node.

A vdisk can be deduped, have various block sizes and depending on the underlying storage cluster, have different speed characteristics.

Components:
* [NBD Server](nbdserver/)
  - A network block device server to expose the vdisks to virtual machines
* [Gonbdserver](gonbdserver/)
  - An NBD server written in Go, integrated within the [NBD Server](nbdserver/)
* [TLOG Server](tlog/)
  - A transaction log server to record block changes
* [Command Line Tool](g8stor/)
  - A command line tool to control vdisks

Make sure to have [Golang](https://golang.org/) version 1.8 or above installed.

## More

All documentation is in the [`/docs`](./docs) directory, including a [table of contents](/docs/SUMMARY.md).

In [Getting Started with NBD Server](/docs/gettingstarted/gettingstarted.md) you find the recommended path to quickly get up and running.
