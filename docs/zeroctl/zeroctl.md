# zeroctl

The 0-Disk command line tool (zeroctl) allows you to manage and discover [vdisks][vdisk].

## Installing zeroctl

Requires `Go 1.8` or above.

Using [`github.com/zero-os/0-Disk/Makefile`](../../Makefile):

```
OUTPUT=$GOPATH/bin make zeroctl
```

## Commands

Here we'll give a quick overview of each command. Each command also has more elaborate documentation which can be accessed by clicking on the command.

Information on how to use each command can also be accessed using `zeroctl help [command]`, e.g. `zeroctl help copy vdisk` will give output how to copy [vdisks][vdisk] using that command.

### [`zeroctl version`](commands/version.md)

Output the zeroctl version and runtime information.

### [`zeroctl copy vdisk`](commands/copy.md#vdisk)

Copy a [vdisk]'s stored [data (1)][data] or [metadata (1,2,3)][metadata] as a new [vdisk][vdisk].

### [`zeroctl delete vdisk`](commands/delete.md#vdisk)

Delete a [vdisk][vdisk]'s stored [data (1)][data] and/or [metadata (1,2,3)][metadata].

### [`zeroctl restore vdisk`](commands/restore.md#vdisk)

[Restore][restore] a [vdisk][vdisk] (as a new [vdisk][vdisk]), using stored transactions for those [vdisks][vdisk] that have [TLog][tlog] support and have enabled it.

### [`zeroctl list vdisks`](commands/list.md#vdisks)

List all available [vdisks][vdisk] on a given [storage (1)][storage] server.

NOTE: this command is slow if used on a [storage (1)][storage] server which has a lot of keys. Use this command with precaution.

### [`zeroctl list snapshots`](commands/list.md#snapshots)

List all available [snapshots][snapshot] on a given backup storage.

### [`zeroctl export vdisk`](commands/export.md#vdisk)

Export a [stored (1)][storage] [vdisk][vdisk] in a secure and efficient manner onto a (S)FTP server, in essense making a [backup][backup] of the [vdisk][vdisk] in question.

### [`zeroctl import vdisk`](commands/import.md#vdisk)

Import a [vdisk][vdisk] [backup][backup] from a (S)FTP server and [store (1)][storage] it as a (new) [vdisk][vdisk].

### [`zeroctl describe snapshot`](commands/describe.md#snapshot)

Describe a [vdisk][vdisk] [backup][backup] (see: snapshot) from a (S)FTP server.

[storage]: /docs/glossary.md#storage
[backup]: /docs/glossary.md#backup
[data]: /docs/glossary.md#data
[metadata]: /docs/glossary.md#metadata
[vdisk]: /docs/glossary.md#vdisk
[tlog]: /docs/glossary.md#tlog
[snapshot]: /docs/glossary.md#snapshot
[restore]: /docs/restore.md#tlog
