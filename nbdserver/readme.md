# NBD Server for blocks stored in ARDB

## Build instructions

Clone the code to your GOPATH:
```
go get -d github.com/g8os/blockstor/nbdserver
```

Build (totally static) the binary:
```
cd $GOPATH/src/github.com/g8os/blockstor/nbdserver
CGO_ENABLED=0
GOOS=linux
go build -a -ldflags '-extldflags "-static"' .
```

Optionally you can now create a flist for your NBD server so you can easily start a container on a G8OS with your NBD server. Below the steps.

First put 'nbdserver' binary somewhere alone:
```
mkdir /tmp/nbdserver
cp nbdserver /tmp/nbdserver
```

Then, using JumpScale:

```
REMOTE_ARDB_SERVER="IP address of an ARDB server"
kvs = j.servers.kvs.getRocksDBStore(name='flist', namespace=None, dbpath="/tmp/flist-nbdserver.db")
f = j.tools.flist.getFlist(rootpath='/tmp/nbdserver', kvs=kvs)
f.add('/tmp/nbdserver/')
f.upload("REMOTE_ARDB_SERVER", 16379)
```

And finally, pack your RocksDB database, and you're done:
```
cd /tmp/flist-nbdserver.db/
tar -cf ../flist-nbdserver.db.tar *
cd ..
gzip flist-nbdserver.db.tar
```

## Configuration

The NBD server and its backend is configured using a YAML configuration file.

The [`ClusterClientFactory`][clusterclientfactory], [`BackendFactory`][backendfactory] and so on, all take a file path to this YAML configuration file.
The `nbdserver` executable as well takes a `--config path`, which is then delegated to the factories mentioned above, among other objects.
Therefore it is important to understand this configuration well, as it controls most of the NBD server behavior.

Here's the `config.yml` file:

```yaml
storageClusters: # A required map of storage clusters,
                 # only 1 storage cluster is required
  mycluster: # Required (string) ID of this storage cluster
    dataStorage: # A required array of connection (dial)strings, used to store data
      - 192.168.58.146:2000 # At least 1 connection (dial)string is required,
      - 192.123.123.123:2001 # more are optional
    metadataStorage: 192.168.58.146:2001 # Required connection (dial)string,
                                         # used to store meta data (LBA indices)
  rootcluster: # Required (string) ID of this (optional) storage cluster
    dataStorage: # A required array of connection (dial)strings, used to store data
      - 192.168.58.147:2000 # only 1 connection (dial)string is required
    metadataStorage: 192.168.58.147:2001 # Required connection (dial)string
  # ... more (optional) storage clusters
vdisks: # A required map of vdisks,
        # only 1 vdisk is required
  myvdisk: # Required (string) ID of this vdisk
    blocksize: 4096 # Required static (uint64) size of each block
    readOnly: false # Defines if this vdisk can be written to or not
                    # (optional, false by default)
    size: 10 # Required (uint64) total size in GiB of this vdisk
    storageCluster: mycluster # Required (string) ID of the storage cluster to use
                              # for this vdisk's storage, has to be a storage cluster
                              # defined in the `storageClusters` section of THIS config file
    rootStorageCluster: rootcluster # Optional (string) ID of the (root) storage cluster to use
                                    # for this vdisk's fallback/root/template storage, has to be
                                    # a storage cluster defined in the `storageClusters` section
                                    # of THIS config file
    type: boot # Required (VdiskType) type of this vdisk
               # which also defines if its deduped or nondeduped,
               # valid types are: `boot`, `db` and `cache`
  # ... more (optional) vdisks
```

As you can see, both the storage clusters and vdisks are configured in
and within the same NBD server `config.yml` file.

By default the `nbdserver` executable assumes the `config.yml` file
exists within the working directory of its process. This location can be defined
using the `--config path` optional CLI flag.

[clusterclientfactory]: /storagecluster/cluster.go#L32-#L40
[backendfactory]: /nbdserver/ardb/ardb.go#L67-L75


## Usage

Make sure you have an ARDB server running on `localhost:16379`.

```
go build && ./nbdserver -protocol tcp -address ":6666"
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
