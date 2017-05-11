# Copyvdisk

Copyvdisk is CLI tool to copy the metadata of a deduped vdisk,
using the G8OS Resource Pool API or the Redis MetaData server(s).

## Installation

```
go install github.com/g8os/blockstor/cmd/copyvdisk
```

Requires `Go 1.8` or above.

## Usage

```
$ copyvdisk -h
copyvdisk 1.1.0

copy the metadata of a deduped vdisk

usage:
  copyvdisk [-v] \
    [-sourcetype api|direct] [-targettype api|direct] \
    [-sourcesc name] [-targetsc name] \
    source_vdisk target_vdisk source_url [target_url]

  -sourcetype, -targettype:
    types of the given url(s), default="api", options:
      => "api": specify an url to use the GridAPI for the source/target;
      => "direct": specify a connection string to use an ARDB directly;

  -sourcesc, -targetsc:
    combined with the "sourcetype and/or targettype" flag(s),
    it allows you to predefine the source's and/or target's storageCluster name,
    instead of fetching it automatically from the GridAPI's vdisk info

  -v:
    log all available info to the STDERR

  -h, --help:
    print this usage message

  When no target_url is given,
  the target_url is the same as the source_url
  and the type of target_url (-targettype)
  will be the same as the type of source_url (-sourcetype).
```

## More

See the [Copyvdisk documentation](/docs/copyvdisk/copyvdisk.md) for more detailed documentation.
