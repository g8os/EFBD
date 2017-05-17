# Using g8stor

```
$ g8stor -h
g8stor controls the g8os resources

Find more information at github.com/g8os/blockstor/g8stor.

Usage:
  g8stor [command]

Available Commands:
  copy        Copy a g8os resource
  delete      Delete a g8os resource
  help        Help about any command
  restore     Restore a g8os resource
  version     Output the version information

Flags:
  -v, --verbose   log available information

Use "g8stor [command] --help" for more information about a command
```

## Examples

### Copy metadata of a deduped vdisk

vdisk `a` and `b` are in the same ardb (`localhost:16379`):

```
$ g8stor copy deduped a b localhost:16379
```

vdisk `a` and `b` are in different ardbs (`localhost:16379` -> `localhost:16380`):

```
$ g8stor copy deduped a b localhost:16379 localhost:16380
```

vdisk `a` and `b` are in different ardb databases (`localhost:16379 DB=0` -> `localhost:16379 DB=1`):

```
$ g8stor copy deduped a b localhost:16379 --targetdb 1
```

### Delete metadata of a deduped vdisk

delete vdisk `a`:

```
$ g8stor delete deduped a localhost:16379
```

### Restore a (deduped or nondeduped) vdisk

restore vdisk `a`:

```
$ g8stor restore vdisk a
```

**Note**: this requires that you have a `config.yml` file in the current working directory.
