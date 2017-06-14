# Using the 0-Disk Command Line Tool

```
$ zeroctl -h
zeroctl controls the zero-os resources

Find more information at github.com/zero-os/0-Disk/zeroctl.

Usage:
  zeroctl [command]

Available Commands:
  copy        Copy a zero-os resource
  delete      Delete a zero-os resource
  help        Help about any command
  list        List zero-os resources
  restore     Restore a zero-os resource
  version     Output the version information

Flags:
  -h, --help      help for zeroctl
  -v, --verbose   log available information

Use "zeroctl [command] --help" for more information about a command.
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

Note that the examples below don't show all available flags.
Please use `zeroctl [command] --help` to see the flags of a specific command,
`zeroctl copy vdisk --help` will for example show all information available for the
command used to copy a vdisk.

## Copy a vdisk

To copy `vdiskA` as a new vdisk (`vdiskB`) on the _same_ storage cluster (`clusterA`), I would do:

```
$ zeroctl copy vdisk vdiskA vdiskB
```

Which would be the same as the more explicit version:

```
$ zeroctl copy vdisk vdiskA vdiskB clusterA --config config.yml
```

To copy `vdiskA` as a new vdisk (`vdiskA`) on a _different_ storage cluster (`clusterB`), I would do:

```
$ zeroctl copy vdisk vdiskA vdiskA clusterB
```

The following command would be illegal, and abort with an error:

```
$ zeroctl copy vdisk vdiskA vdiskA
```

## Delete vdisks

To delete all vdisks listed in the config file:

```
$ zeroctl delete vdisks
```

Which is the less explicit version of:

```
$ zeroctl delete vdisks --config config.yml
```

To delete only 1 (or more) vdisks, rather then all, we can specify their id(s):

```
$ zeroctl delete vdisks vdiskC --config.yml
```

With this knowledge we can write the first delete example even more explicit:

```
$ zeroctl delete vdisks vdiskA vdiskC --config.yml
```

The following would succeed for the found vdisk, but log an error for the other vdisk as that one can't be found:

```
$ zeroctl delete vdisks foo vdiskA # vdiskA will be deleted correctly, even though foo doesn't exist
```

### Restore a (deduped or nondeduped) vdisk

Restore a whole vdisk `a`:

```
$ zeroctl restore vdisk a
```
There are also optional timestamp (in nanosecond) options

Restore vdisk `a` from timestamp `x` to the end

```
$ zeroctl restore vdisk a --start-timestamp=x
```

Restore vdisk `a` from the start until timestamp `x`

```
$ zeroctl restore vdisk a --end-timestamp=x
```


**Note**: this requires that you have a `config.yml` file in the current working directory.

### List all available vdisks

List vdisks available on `localhost:16379`:

```
$ zeroctl list vdisks localhost:16379
```

#### WARNING

This command is slow, and might take a while to finish!
It might also decrease the performance of the ARDB server
in question, by locking the server down for each operation.
