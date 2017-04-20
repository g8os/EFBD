# CopyVdisk CLI

A CLI to copy the metadata of a deduped vdisk,
using the GridAPI or the Redis MetaData server(s).

## Docs

### Install

```
$ go install github.com/g8os/blockstor/cmd/copyvdisk
```

Requires `Go 1.8` or above.

### Usage

```
$ copyvdisk -h

usage:
  copyvdisk [-v] [-t api|direct] source_vdisk target_vdisk source_url [target_url]
```

  When no target_url is given, the target metadataserver is the same as the source metadataserver.

+ `[-v]`: log all (progress) info to STDERR;
+ `[-t api|direct]`: define the type of the given url(s), the gridapi url's or the direct metadataserver connectionstrings;
+ `[-sourcesc name]`: combined with api type it allows you to predefine the source's storageCluster name;
+ `[-targetsc name]`: combined with api type it allows you to predefine the target's storageCluster name;
