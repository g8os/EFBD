# Zero-OS 0-Disk [![Build Status](https://travis-ci.org/zero-os/0-Disk.svg?branch=master)](https://travis-ci.org/zero-os/0-Disk)

This repository implements the components required for supporting block storage in Zero-OS nodes.

The Zero-OS block storage components allow you to create and use block devices (vdisks) from within virtual machines hosted on a Zero-OS node.

![0-Disk overview](/docs/assets/zerodisk_overview.png)

A vdisk can be deduped, persistent, redundant and depending on the underlying storage, have different speed characteristics.

Components:
* [NBD Server](nbdserver/)
  - A network block device (NBD) server to expose the vdisks to virtual machines
* [TLOG Server/Client](tlog/)
  - A transaction log server and client to record block changes
* [zeroctl](zeroctl/)
  - A command line tool to manage vdisks

Make sure to have [Golang](https://golang.org/) version 1.8 or above installed.

## More

All documentation is in the [`/docs`](docs/) directory, including a [table of contents](/docs/SUMMARY.md) and a [glossary of terminology](/docs/glossary.md) used in this project.

All code is also documented, which can be found at [godoc](http://godoc.org/github.com/zero-os/0-Disk).

In [Getting Started with NBD Server](/docs/gettingstarted/gettingstarted.md) you find the recommended path to quickly get up and running.
