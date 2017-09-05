# NBD Server for blocks stored in ARDB

## Installing the NBD Server

Requires `Go 1.8` or above.

Using [`github.com/zero-os/0-Disk/Makefile`](../../Makefile):

```
OUTPUT=$GOPATH/bin make nbdserver
```

or by simply using the Go toolchain:

```
go install github.com/zero-os/0-Disk/nbdserver
```

## NBD Server Configuration

See [the NBD Server Configuration docs](/docs/nbd/config.md) for more information about how to configure the NBD Server.

## Usage

Use `nbdserver -h` or `nbdserver --help` to get more information about all available flags.

### Example

Make sure you have an ARDB server(s) running, on the connection info specified in the used configured (using configuration stored in the etcd server running at `myserver:2037`).

```
make nbdserver && bin/nbdserver -protocol tcp -address ":6666" -config myserver:2037
```

Connect your `nbd-client` to the server running on `localhost:6666`:

```
sudo nbd-client -b 4096 -name default localhost 6666 /dev/nbd1
sudo mkfs.ext4 -b 4096 /dev/nbd1
sudo mount /dev/nbd1 /mnt/sharedvolume
```

Converting an image using 'qemu-img' to insert an image in the NBD server:

```
apt-get install qemu-utils
```

When the NBD server is running on a TCP socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+tcp://[HOST]:[PORT]/[VDISKID]
```

When the NBD server is running on a Unix socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -q nbd nbd+unix:///[VDISKID]?socket=/tmp/nbd-socket
```

## More

For more verbose documentation see the [`/docs`](/docs) directory.

You'll find there following sections about the NBD Server:

- [NBD Server Introduction](/docs/nbd/nbd.md)
- [Building your NBD Server](/docs/nbd/building.md)
- [NBD Server Configuration](/docs/nbd/config.md)
- [Using your NBD Server](/docs/nbd/using.md)

For more information about the internals of the NBD Server you can find the following sections:

- [The types of Backend Storage and how they work](/docs/nbd/backendstorage.md)
- [The LBA Lookups of the Deduped Backend Storage](/docs/nbd/lbalookups.md)
