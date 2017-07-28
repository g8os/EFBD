/*Package config is a package for config management for 0-Disk virtual disks.

Config

The config data is stored in 4 subconfigs: base(general vdisk information), nbd (nbd server information), tlog (tlog server information) and slave (slave config information). The information is stored internally in YAML format.

Each subconfig has a constructor, ToBytes and a Validate methode.
The constructors will unmarshal a subconfig from on a YAML formatted slice of byte, validates the subconfig and returns an error if invalid.
ToBytes will marchal the subconfig to a YAML slice of bytes and return it.
Validate will validate the current subconfig. It is however only recommended to be used before setting a subconfig to a source in that source implementation for the configs.

ETCD source

The etcd config source can be used for production allowing for distributed storage of the configs. From the provided endpoint(s) it will connect to the etcd cluster and get the subconfigs according to following keys:
	base: <VdiskID>:conf:base
	nbd: <VdiskID>:conf:nbd
	tlog: <VdiskID>:conf:tlog
	slave: <VdiskID>:conf:slave

The Get Read<Sub>ConfigETCD functions will connect to the etcd store and get the data for the subconfig. It uses the subconfig's constructor to create and validate the subconfig and returns an error if this or the connection to the etcd store fails.

The Watch<Sub>ConfigETCD functions uses the etcdv3 Watch API to listen for updates of the  subconfig and send that updated value if valid to the channel returned from the function.
When calling this function it will return an error if no connection could be made or the current data is invalid. If no error occurred it will return a channel and send the current subconfig to the channel.
The configs stored at the etcd keys are YAML formatted serialized versions of the subconfig.

NOTE: For the NBDConfig there should be a valid BaseConfig available in the source (e.g. etcd), as it is required to validate the NBDConfig.

File source

The fileConfig source is recommended for development use and gets it's config data from a provided YAML file path. 1 YAML file represents the data of 1 vdisk formatted to the yamlConfigFormat type.
It will automatically devide the data in the subconfigs after reading from the source file.

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

The Get Read<Sub>ConfiFile functions will the source file and get the data for the subconfig. It uses the subconfig's constructor to create and validate the subconfig and returns an error if this or the reading the file fails.

The Watch<Sub>ConfigFile functions listens to the SIGHUP signal for updates of the configs and send that updated value if valid to the channel returned from the function.
When calling this function it will return an error if it could not read the source file or the current data is invalid. If no error occurred it will return a channel and send the current subconfig to the channel.

NOTE: For the NBDConfig there should be a valid BaseConfig available in the source (e.g. file), as it is required to validate the NBDConfig.

*/
package config
