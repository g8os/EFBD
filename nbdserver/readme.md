# NBDServer for blocks stored in ARDB

## Configuration

The NBDServer and its backend, is to be configured using a YAML Config file.
The [`ClusterClientFactory`][clusterclientfactory], [`BackendFactory`][backendfactory] and so on, all take a filepath to this YAML Config file.
The NBDServer as well takes a `--config path`, which is then delegated to the factories mentioned above, among other objects.
Therefore it is important to understand this configuration well, as it controls most of the NBDServer.

### YAML Configuration

```yaml
storageclusters: # A required map of storage clusters
  mycluster: # Required (string) ID of this storage cluster
    dataStorage: # A required array of connection (dial)strings, used to store data
      - 192.168.58.146:2000 # At least 1 connection (dial)string is required,
      - 192.123.123.123:2001 # more are optional
    rootDataStorage: 192.168.2.2:2002 # Optional connection (dial)string
                                      # of fallback/root storage
    metadataStorage: 192.168.58.146:2001 # Required connection (dial)string,
                                         # used to store meta data (LBA indices)
  # ... more (optional) storage clusters
vdisks: # A required map of vdisks
  myvdisk: # Required (string) ID of this vdisk
    blocksize: 4096 # Required static (uint64) size of each block
    readOnly: false # Defines if this vdisk can be written to or not
                    # (optional, false by default)
    size: 10 # Required (uint64) total size in GiB of this vdisk
    storagecluster: mycluster # Required (string) ID of storage cluster to use
                              # for this vdisk's storage, has to be a storage cluster
                              # defined in the `storageclusters` section of THIS config file
    type: boot # Required (VdiskType) type of this vdisk
               # which also defines if its deduped or nondeduped,
               # valid types are: `boot`, `db` and `cache`
  # ... more (optional) vdisks
```

As you can see, both the storage clusters and vdisks are configured in
and within the same nbdserver `config.yaml` file.

By default the NBDServer CLI util assumes the `config.yaml` file
exists within the work directory of its process, this location can be defined
using the `--config path` optional CLI flag.

## Test locally

Make sure you have an ardb server running on `localhost:16379`!

```
go build && ./nbdserver -protocol tcp -address ":6666"
```

Connect your nbdclient to the server running on `localhost:6666`:

```
sudo nbd-client -b 4096 -name default localhost 6666 /dev/nbd1
sudo mkfs.ext4 -b 4096 /dev/nbd1
sudo mount /dev/nbd1 /mnt/sharedvolume
```

Converting an image using qemu-img to insert an image in the nbdserver:

When the nbdserver is running on a tcp socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+tcp://[HOST]:[PORT]/[VDISKID]
```

When the nbdserver is running on a unix socket:
```
qemu-img convert -p -n ubuntu-16.04-server-cloudimg-amd64-disk1.img -O nbd nbd+unix:///[VDISKID]?socket=/tmp/nbd-socket
```

[clusterclientfactory]: ../storagecluster/cluster.go#L32-#L40
[backendfactory]: ./ardb/ardb.go#L67-L75
