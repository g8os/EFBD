# CopyVolume CLI

A CLI to copy the metadata of a deduped volume,
using the GridAPI or the Redis MetaData server(s).

## Docs

### Install

```
$ go install github.com/g8os/blockstor/cmd/copyvolume
```

Requires `Go 1.8` or above.

### Usage

```
$ copyvolume -h

usage:
  copyvolume [-v] [-t grid|meta] source_volume target_volume source_url
  copyvolume [-v] [-t grid|meta] source_volume target_volume source_url target_url
```

+ `[-v]`: log all (progress) info to STDERR;
+ `[-t grid|meta]`: define the type of the given url(s);
