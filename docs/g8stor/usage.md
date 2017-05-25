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

Config file used in examples where the config file is used:

```yaml
storageClusters:
  clusterA:
    dataStorage:
      - address: localhost:6379
    metaDataStorage:
      address: localhost:6379
  clusterB:
    dataStorage:
      - address: localhost:6380
    metaDataStorage:
      address: localhost:6380
vdisks:
  vdiskA:
    blockSize: 4096
    size: 1
    storageCluster: clusterA
    type: boot
  vdiskC:
    blockSize: 4096
    size: 1
    storageCluster: clusterB
    type: boot
```

## Copy a vdisk

to copy `vdiskA` as a new vdisk (`vdiskB`) on the _same_ storage cluster (`clusterA`), I would do:

```
$ g8stor copy vdisk vdiskA vdiskB
```

which would be the same as the more explicit version:

```
$ g8stor copy vdisk vdiskA vdiskB clusterA --config config.yml
```

to copy `vdiskA` as a new vdisk (`vdiskA`) on a _different_ storage cluster (`clusterB`), I would do:

```
$ g8stor copy vdisk vdiskA vdiskA clusterB
```

the following command would be illegal, and abort with an error:

```
$ g8stor copy vdisk vdiskA vdiskA
```

## Delete vdisks

to delete all vdisks listed in the config file:

```
$ g8stor delete vdisks
```

which is the less explicit version of:

```
$ g8stor delete vdisks --config config.yml
```

to delete only 1 (or more) vdisks, rather then all, we can specify their id(s):

```
$ g8stor delete vdisks vdiskC --config.yml
```

with this knowledge we can write the first delete example even more explicit:

```
$ g8stor delete vdisks vdiskA vdiskC --config.yml
```

the following would succeed for the found vdisk, but log an error for the other vdisk as that one can't be found:

```
$ g8stor delete vdisks foo vdiskA # vdiskA will be deleted correctly, even though foo doesn't exist
```

### Restore a (deduped or nondeduped) vdisk

restore vdisk `a`:

```
$ g8stor restore vdisk a
```

**Note**: this requires that you have a `config.yml` file in the current working directory.


## Legacy Examples

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