/*Package configV2 is a package for config management for 0-Disk virtual disks.

Config

The config data is stored in 4 sub configs: base(general vdisk information), nbd (nbd server information), tlog (tlog server information) and slave (slave config information). The information is stored internally in YAML format.

Each sub config has a constructor, ToBytes and a Validate methode.
The constructor will unmarshal a sub config from on a YAML formatted slice of byte, validates the sub config and returns an error if invalid.
ToBytes will marchal the subconfig to a YAML slice of bytes and return it.
Validate will validate the current subconfig. It is however only recommended to be used before setting a subconfig in the ConfigSource implementation.

NBDConfig, TlogConfig and SlaveConfig each have a Clone methode that will return a deep copy of their pointer type

ConfigSource

ConfigSource is an interface that defines how a wrapper can be implemented allowing for different sources of where the sub configs may be stored while still being able to use the same API independent of the source.

BaseConfig should be required, the others can be optional and return ErrConfigNotAvailable when trying to get a config that is not set(nil).

It's recommended to use the Close method of the ConfigSource after use to properly close potential goroutine and channels used for updating the config data in the background.

2 Implementations of the ConfigSource interface are included in this package allowing to get and/or save config to a YAML file(file) or etcd cluster(etcd).
They both update their sources insignaling the background and update their values if their sources have been updated, if properly triggered (SIGHUP for file, watch for etcd), when new values are set, they will send a trigger signalling other configs using the same source to update.

File source

The fileConfig source is recommended for development use and gets it's config data from a provided YAML file path. 1 YAML file represents the data of 1 vdisk formatted to the yamlConfigFormat type.
It will automatically devide the data in the sub configs after reading and the source file.

YAML file example:
	baseConfig:
		blockSize: 4096
		readOnly: false
		size: 10
		type: db
	nbdConfig:
		templateVdiskID: mytemplate
		storageCluster:
			dataStorage:
			- address: 192.168.58.146:2000
				db: 0
			- address: 192.123.123.123:2001
				db: 0
			metadataStorage:
			address: 192.168.58.146:2001
			db: 1
		templateStorageCluster:
			dataStorage:
			- address: 192.168.58.147:2000
				db: 0
	tlogConfig:
		tlogStorageCluster:
			dataStorage:
		current	- address: 192.168.58.149:2000
				db: 4
			metadataStorage:
			address: 192.168.58.146:2001
			db: 8

Setting one of the sub configs will write the entire current config to that source file and trigger a SIGHUP signal, letting other configs know the source has been updated.

ETCD source

The etcdConfig source can be used for production allowing for distributed storage of the configs. From the provided endpoint(s) it will connect to the etcd cluster and get the subconfigs according to following keys:
	base: <vdiskID>:conf:base
	nbd: <vdiskID>:conf:nbd
	tlog: <vdiskID>:conf:tlog
	slave: <vdiskID>:conf:slave
Using the etcd3 Watch API, etcdConfig will listen for updates of each subconfig and update the local configs as they come in.
The config stored at the etcd keys are YAML formatted serialized versions of the sub config.
*/
package configV2
