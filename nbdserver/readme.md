# NBD Server for blocks stored in ARDB

## NBD Server Configuration

The NBD server and its backend is configured using a YAML configuration file:

```yaml
storageClusters: # A required map of storage clusters,
                 # only 1 storage cluster is required
  mycluster: # Required (string) ID of this storage cluster,
             # you are free to name the cluster however you want
    dataStorage: # A required array of connection (dial)strings, used to store data
      - address: 192.168.58.146:2000 # At least 1 connection (dial)string is required
        db: 0                        # database is optional, 0 by default
      - address: 192.123.123.123:2001 # more connections are optional
    metadataStorage: # Required ONLY when used as the (Root)StorageCluster of a `boot` vdisk
      address: 192.168.58.146:2001 # Required connection (dial)string,
                                   # used to store meta data (LBA indices)
  rootcluster: # Required (string) ID of this (optional) storage cluster,
               # you are free to name the cluster however you want
    dataStorage: # A required array of connection (dial)strings, used to store data
      - address: 192.168.58.147:2000 # only 1 connection (dial)string is required
        db: 1                        # database is optional, 0 by default
    metadataStorage: # Required ONLY when used as the (Root)StorageCluster of a `boot` vdisk
      address: 192.168.58.147:2001 # Required connection (dial)string
  # ... more (optional) storage clusters
vdisks: # A required map of vdisks,
        # only 1 vdisk is required,
        # the ID of the vdisk is the same one that the user of this vdisk (nbd client)
        # used to connect to this nbdserver
  myvdisk: # Required (string) ID of this vdisk
    blockSize: 4096 # Required static (uint64) size of each block
    readOnly: false # Defines if this vdisk can be written to or not
                    # (optional, false by default)
    size: 10 # Required (uint64) total size in GiB of this vdisk
    storageCluster: mycluster # Required (string) ID of the storage cluster to use
                              # for this vdisk's storage, has to be a storage cluster
                              # defined in the `storageClusters` section of THIS config file

    slaveStorageCluster: slaveCluster # Optional (string) ID of the storage cluster to use
                                      # for this vdisk's storage, has to be a storage cluster
                                      # defined in the `storageClusters` section of THIS config file
    rootStorageCluster: rootcluster # Optional (string) ID of the (root) storage cluster to use
                                    # for this vdisk's fallback/root/template storage, has to be
                                    # a storage cluster defined in the `storageClusters` section
                                    # of THIS config file
    rootVdiskID: mytemplate # Optional (string) ID of the template vdisk,
                            # only used for `db` vdisks
    type: boot # Required (VdiskType) type of this vdisk
               # which also defines if its deduped or nondeduped,
               # valid types are: `boot`, `db`, `cache` and `tmp`
  # ... more (optional) vdisks

tlogrpc: 127.0.0.1:11211,127.0.0.1:11221 # addresses of the tlogrpc server.
                                         # it is optional and going to be ignored if `tlogrpc` command 
                                         # line option is specified
```

As you can see, both the storage clusters and vdisks are configured in
and within the same NBD server `config.yml` file.

By default the `nbdserver` executable assumes the `config.yml` file
exists within the working directory of its process. This location can be defined
using the `--config path` optional CLI flag.

[clusterclientfactory]: /storagecluster/cluster.go#L32-#L40
[backendfactory]: /nbdserver/ardb/ardb.go#L67-L75

### Live reloading of the configuration

A running nbdserver in a production environment can not simply be restarted
since this will break the connection to any connected client.
When the configuration file is modified,
send a `SIGHUP` signal to the nbdserver to make it pick up the changes.

**NOTE**: It is not recommended to change the configs of storage clusters,
whichare still in use by active (connected) clients,
and content might get lost if you do this anyway.

## Usage

Use `nbdserver -h` or `nbdserver --help` to get more information about all available flags.

### Example

Make sure you have an ARDB server(s) running, on the connection info specified in the used `config.yml` file.

```
make nbdserver && bin/nbdserver -protocol tcp -address ":6666"
```

You can also instead simply run the nbdserver with no flag specified `bin/nbdserver`
to run it on the default `/tmp/nbd-socket` unix socket,
which is the same as the more explicit version `bin/nbdserver -protocol unix --address /tmp/nbd-socket`.

Note that if you don't have the `config.yml` file in your current working directory,
you'll have to specify the config file explicitly using the `-config path` flag.

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
