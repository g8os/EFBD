# NBD Server Configuration

This documentation supplements the configuration instructions in the [README](/nbdserver/readme.md) of the [`nbdserver`](/nbdserver) source directory.

The NBD server and its backend is configured using a YAML configuration file.

The [`ClusterClientFactory`][clusterclientfactory], [`BackendFactory`][backendfactory] and so on, all take a file path to this YAML configuration file.
The `nbdserver` executable as well takes a `--config path`, which is then delegated to the factories mentioned above, among other objects.
Therefore it is important to understand this configuration well, as it controls most of the NBD server behavior.

Here's the `config.yml` file:

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
```

As you can see, both the storage clusters and vdisks are configured in
and within the same NBD server `config.yml` file.

By default the `nbdserver` executable assumes the `config.yml` file
exists within the working directory of its process. This location can be defined
using the `--config path` optional CLI flag.

[clusterclientfactory]: /storagecluster/cluster.go#L32-#L40
[backendfactory]: /nbdserver/ardb/ardb.go#L67-L75
