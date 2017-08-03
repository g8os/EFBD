# Import YAML File Config into etcd Cluster

This tool is meant to import a file-based 0-Disk configuration,
into an etcd cluster. This tool is only meant for development and testing purposes,
and should not be used in production.

Check out [the 0-Disk configuration docs](/docs/config.md) for more information about
how the 0-Disk configuration works.

## Usage

```
Usage: import_file_into_etcd_config [flags] etcd_endpoint...
  -f	when true, overwrite config values without asking first (default: false)
  -id string
    	unique id of the server that will use the etcd cluster (default "default")
  -path string
    	path to yaml config file (default: config.yml) (default "config.yml")
```

You can simply run the tool using:

```
$ go run tools/import_file_into_etcd_config --help
```

Or you can first build it, and than use it:

```
$ go build tools/import_file_into_etcd_config
./import_file_into_etcd_config --help
```

## Example

Assuming we have following config as `./config.yml`:

```yaml
storageClusters:
  mycluster:
    dataStorage:
      - address: localhost:16379
      - address: localhost:16380
    metadataStorage:
      address: localhost:16381
  8207316437363886335:
    dataStorage:
      - address: localhost:16382
    metadataStorage:
      address: localhost:16382
vdisks:
  vd2:
    type: boot
    blockSize: 4096
    size: 4
    nbd:
      storageClusterID: mycluster
      templateStorageClusterID: 8207316437363886335
  template:ubuntu-1604:
    type: boot
    blockSize: 4096
    size: 4
    nbd:
      storageClusterID: 8207316437363886335
```

We can run this tool as follows:

```
$ ./import_file_into_etcd_config -id <ServerID> <ETCDEndpoint>
```

which if all goes well should give us the following output:

```
written config to 'mycluster:cluster:conf:storage':
dataStorage:
- address: localhost:16379
  db: 0
- address: localhost:16380
  db: 0
metadataStorage:
  address: localhost:16381
  db: 0

written config to '8207316437363886335:cluster:conf:storage':
dataStorage:
- address: localhost:16382
  db: 0
metadataStorage:
  address: localhost:16382
  db: 0

written config to 'vd2:vdisk:conf:static':
blockSize: 4096
readOnly: false
size: 4
type: boot
templateVdiskID: ""

written config to 'vd2:vdisk:conf:storage:nbd':
storageClusterID: mycluster
templateStorageClusterID: "8207316437363886335"
slaveStorageClusterID: ""
tlogServerClusterID: ""

written config to 'template:ubuntu-1604:vdisk:conf:static':
blockSize: 4096
readOnly: false
size: 4
type: boot
templateVdiskID: ""

written config to 'template:ubuntu-1604:vdisk:conf:storage:nbd':
storageClusterID: "8207316437363886335"
templateStorageClusterID: ""
slaveStorageClusterID: ""
tlogServerClusterID: ""

written config to 'vm2:nbdserver:conf:vdisks':
vdisks:
- vd2
- template:ubuntu-1604

etcd has all file-originated configs, no errors occured in the process
```

If one of the subconfigs would already exist the tool will ask you if it should be overwritten:

```
Their Config:
vdisks:
- vd2

Our Config:
vdisks:
- vd2
- template:ubuntu-1604


overwrite the existing config vm2:nbdserver:conf:vdisks [y/n]:
```

On which you can respond `y` or `n`. You can also auto-reply `y` on all such questions,
by specifying the `-f` flag to force write all configs, no matter what the situation is.
