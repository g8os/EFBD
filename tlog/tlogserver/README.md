# Go Tlog Server

## Tlog Server Configuration

The Tlog server is configured using a YAML configuration file:

```yaml
storageClusters: # A required map of storage clusters
  tlogcluster: # required (string) ID of this storage cluster,
               # you are free to name the cluster however you want
    dataStorage: # A required array of connection (dial) strings, used to store data,
                 # NOTE that storage clusters used for tlog purposes,
                 #      require at least K+M servers, rather than just the normal minimum of 1,
                 #      this is not validated by the config file loader,
                 #      but will result in a tlogclient handshake error,
                 #      in case there are insufficient (N < K+M) dataStorage servers listed
                 # in this example K=2 and M=2, thus we require 4 servers,
                 # extra servers (I >= K+M) are allowed, but ignored
     - address: 192.168.58.148:2000 # Required connection (dial) string
       db: 0                        # Database is optional, 0 by default
     - address: 192.168.58.148:2000 # Required connection (dial) string
       db: 1                        # Database is optional
     - address: 192.168.58.148:2000 # Required connection (dial) string
       db: 2                        # Database is optional
     - address: 192.168.58.148:2000 # Required connection (dial) string
       db: 3                        # Database is optional
  # ... more (optional) storage clusters
vdisks: # A required map of vdisks,
        # only 1 vdisk is required
  myvdisk: # Required (string) ID of this vdisk
    tlogStorageCluster: tlogcluster # (String) ID of the tlog storage cluster to use
                                    # for this vdisk's tlog's aggregation storage,
                                    # NOTE that this property is REQUIRED in case
                                    # you have a tlogserver connected to your nbdserver
    tlogSlaveSync: true # true if tlog need to sync ardb slave,
                        # optional and false by default
  # ... more (optional) vdisks
```

By default the `tlogserver` executable assumes the `config.yml` file
exists within the working directory of its process. This location can be defined
using the `--config path` optional CLI flag.

### Live reloading of the configuration

A running tlogserver in a production environment can not simply be restarted
since this will break the connection to any connected client.
When the configuration file is modified,
send a `SIGHUP` signal to the tlogserver to make it pick up the changes.

**NOTE**: It is not recommended to change the configs of storage clusters,
whichare still in use by active (connected) clients,
and content might get lost if you do this anyway.

## Usage

Use `tlogserver -h` or `tlogserver --help` to get more information about all available flags.

## erasure coding

It use [isa-l](https://github.com/01org/isa-l) C library and [templexxx/reedsolomon](https://github.com/templexxx/reedsolomon) Go library for erasure coding.

Only one can be used at a time.

By default, the erasure coding is done in go.

When using the C isa-l library for the erasure coding, `-tags isal` needs be passed to go build.
And `GODEBUG=cgocheck=0` environment variable need to be set in order to run it.


**Build**

From this repo root directory
```
make tlogserver
```


**Usage**

Run it
```
./bin/tlogserver  -storage-addresses=127.0.0.1:16379 -k 16 -m 4
```

It starts tlog server that:
- listen on default listen address 0.0.0.0:11211
- need 16 data shards and 4 parity/coding shards -> total 20 ardb 
- first ardb address is `127.0.0.1:16379`, the seconds is in same IP but in port `16380`, the third in port `16381`, and so on...

To specify each ardb adddress, use array as address, example:`-storage-addresses=127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381` to specify 3 ardbs

Use tlog client as described in [client readme](../tlogclient/readme.md) to send transaction log to this tlog server.


**benchmark**
```
GODEBUG=cgocheck=0 go test -tags isal -bench=.
```
