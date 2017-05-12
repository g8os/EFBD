# g8stor

g8stor controls the g8os resources.

It can be used to copy and delete vdisks.

## Installation

```
go install github.com/g8os/blockstor/g8stor
```

Or using the [`github.com/g8os/blockstor/Makefile`](../Makefile).

Requires `Go 1.8` or above.

## Usage

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
  version     Output the version information

Flags:
  -v, --verbose   log available information

Use "g8stor [command] --help" for more information about a command.
```

### Examples

#### Copy metadata of a deduped vdisk

vdisk `a` and `b` are in the same ardb:

```
g8stor copy deduped a b localhost:16379
```

vdisk `a` and `b` are in different ardbs:

```
g8stor copy deduped a b localhost:16379 localhost:16380
```

#### Delete metadata of a deduped vdisk

delete vdisk a:


```
g8stor delete deduped a localhost:16379
```

## More

See the [Copyvdisk documentation](/docs/copyvdisk/copyvdisk.md) for more detailed documentation.
