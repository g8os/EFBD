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
  copyvolume [-v] [-t api|direct] source_volume target_volume source_url [target_url]
```

  When no target_url is given, the target metadataserver is the same as the source metadataserver.

+ `[-v]`: log all (progress) info to STDERR;
+ `[-t api|direct]`: define the type of the given url(s); the gridapi url's or the direct metadataserver connectionstrings.
