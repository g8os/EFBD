/*Package config defines a package used for managing the configuration of 0-Disk services

The config package is split up into 3 major parts: source for the different config source options,
config API that sets up an API for each subconfig, giving each a Read and Watch function and
composition API that sets up the same API combining certain subconfigs to enable direct access the complete config
of the different storage types (NBD/Tlog), giving each a Read and Watch function as well.

No Write or Set function is provided as the 0-Disk should only read the config. However
for testing purposes a stub source with setters is provided.

Source

Config sources are provided allowing to get the configuration from different sources,
intended for different purposes.
A Source interface is defined allowing for the config to interact with the different sources.

The etcd source (intended for production) allows for getting the config from an etcd(v3) cluster (https://github.com/coreos/etcd).
An etcd source needs 1 or more endpoints (dailstrings) to connect to an etcd cluster.
Each subconfig for a vdisk is stored in different keys on the etcd server,
the keys for each subconfig can be found in the etcd section of the 0-Disk config documentation: https://github.com/zero-os/0-Disk/blob/master/docs/config.md#etcd.
The values stored in those keys are expected to be valid subconfigs serialised in YAML format.
In case a key's data was not found valid, the MarkInvalidKey function will be called,
which will log an error as well as broadcast the error to the 0-Orchestrator.
For updates of the config, the etcd source uses the etcd Watch API (https://coreos.com/etcd/docs/latest/learning/api.html#watch-api)
for each etcd key and sends the updated config key value to the channel returned by the Watch function.

The file source (intended for development) allows for getting the config from a YAML file.
A file source needs a file path to where the config file can be found.
How the YAML file should be formatted can be found in the file section of the 0-Disk config documentation: https://github.com/zero-os/0-Disk/blob/master/docs/config.md#file.
For updates of the config, the file source will listen for a SIGHUP signal,
each config key being watched by the Watch function will then read the config file and
send the current subconfig in the file to the channel returned by the Watch function.

The stub source (intended for testing) allows for tests to use a stubbed config where user has control
of the data in the source, the stub source can be written to using the setters
while other sources are not writable from 0-Disk, these setters also trigger a reload,
sending the newly set values to the config keys that are being watched using the Watch function.

SourceConfig is a wrapper source type that switches between a file source or etcd source
based on the provided input string. If it is a dailstring or list of dailstrings,
it will assume that the source should be an etcd source.
If it is an empty string, it will use the default file resource (config.yml) to create a file source.
If it was none of the above it will asume the string is a path to a YAML file for a file source.

Config API

The config API allows to interact with the subconfigs found on the source.
For each subconfig a Read and Watch function is provided.

The Read will read the current subconfig from the provided source and vdisk/cluster/server ID and returns it when valid.

The Watch function creates a goroutine that listens for updates from the provided source
on the config key of the subconfig and provided vdisk/cluster/server ID.
The go routine will close when the provided context is cancelled.
Should the provided context be nil, it will set the internal context to Background and
runs the goroutine for as long as the program remains running.

Composition API

The composition API provides Read and Watch functions for getting full NBD or Tlog storage server configuration,
combining the subconfigs required for each storage type and they are used in exactly the same way as the config API.

The NBD storage consists of following subconfigs:
Static vdisk config (VdiskStaticConfig),
NBD vdisk config (VdiskNBDConfig),
primary storage cluster config (StorageClusterConfig),
template storage cluster config (StorageClusterConfig),
slave storage cluster config (StorageClusterConfig)

The Tlog storage consists of following subconfigs:
Static vdisk config (VdiskStaticConfig),
Tlog vdisk config (VdiskTlogConfig),
0-Stor cluster config (ZeroStorClusterConfig),
slave storage cluster config (StorageClusterConfig)

Some of the subconfigs are optional and are only used for certain storage types.

The Read function will return a full NBDStorageConfig/TlogStorageConfig
from provided source, vdisk ID and static configuration if the configuration is valid.

The Watch function will update the subconfigs through their Watch functions as they are received from the source and
send an updated storage server config to the channel returned fron the Watch function.

More Information

More information about the config module can be found in the 0-Disk config documentation: https://github.com/zero-os/0-Disk/blob/master/docs/config.md
*/
package config
