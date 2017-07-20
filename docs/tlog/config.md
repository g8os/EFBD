# Tlog Server Configuration

The [Tlog server][tlogserver] is configured using a YAML configuration file:

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

## Live reloading of the configuration

A running [tlogserver][tlogserver] in a production environment can not simply be restarted
since this will break the connection to any connected [client][tlogclient].
When the configuration file is modified,
send a `SIGHUP` signal to the [tlogserver][tlogserver] to make it pick up the changes.

> NOTE: It is not recommended to change the configs of [storage (1)][storage] clusters,
which are still in use by active (connected) [clients][tlogclient],
and content might get lost if you do this against all advice in.


[tlogserver]: server.md
[tlogclient]: client.md

[storage]: /docs/glossary.md#storage