package config

var validNBDStorageConfigs = []NBDStorageConfig{
	// complete example
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
				StorageServerConfig{
					Address:  "localhost:16380",
					Database: 2,
				},
			},
		},
		TemplateStorageCluster: &StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address:  "123.123.123.123:300",
					Database: 4,
				},
				StorageServerConfig{
					Address:  "123.123.123.124:300",
					Database: 2,
				},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address:  "foo:16320",
					Database: 4,
				},
				StorageServerConfig{
					Address:  "foo:16321",
					Database: 5,
				},
			},
		},
	},
	// example to show that there can be more data slave servers than required
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address:  "foo:16320",
					Database: 4,
				},
				StorageServerConfig{
					Address:  "foo:16321",
					Database: 5,
				},
			},
		},
	},
	// minimal example
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
	},
}

var invalidNBDStorageConfigs = []NBDStorageConfig{
	// missing primary storage server
	NBDStorageConfig{},
	// invalid primary storage server (no data storage given)
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{},
	},
	// template storage given, but is invalid
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16381"},
			},
		},
		TemplateStorageCluster: new(StorageClusterConfig),
	},
	// slave storage given, but is invalid (no data servers given)
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16381"},
			},
		},
		SlaveStorageCluster: new(StorageClusterConfig),
	},
	// slave storage given, but is invalid (not enough data servers given)
	NBDStorageConfig{
		StorageCluster: StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16381"},
				StorageServerConfig{Address: "localhost:16382"},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{Address: "localhost:16383"},
			},
		},
	},
}

var validTlogStorageConfigs = []TlogStorageConfig{
	// complete example
	TlogStorageConfig{
		SlaveStorageCluster: &StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address:  "localhost:16379",
					Database: 1,
				},
				StorageServerConfig{
					Address:  "localhost:16380",
					Database: 2,
				},
			},
		},
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []ServerConfig{
				ServerConfig{Address: "1.1.1.1:11"},
				ServerConfig{Address: "2.2.2.2:22"},
			},
			MetadataServers: []ServerConfig{
				ServerConfig{Address: "3.3.3.3:33"},
			},
		},
	},
	// minimal example
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []ServerConfig{
				ServerConfig{Address: "1.1.1.1:11"},
				ServerConfig{Address: "2.2.2.2:22"},
			},
			MetadataServers: []ServerConfig{
				ServerConfig{Address: "3.3.3.3:33"},
			},
		},
	},
}

var invalidTlogStorageConfigs = []TlogStorageConfig{
	// missing Storage Cluster
	TlogStorageConfig{},
	// invalid ZeroStor Cluster
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []ServerConfig{
				ServerConfig{Address: "1.1.1.1:11"},
				ServerConfig{Address: "2.2.2.2:22"},
			},
			MetadataServers: []ServerConfig{
				ServerConfig{Address: "3.3.3.3:33"},
			},
		},
	},
	// invalid ZeroStor Storage Cluster
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []ServerConfig{
				ServerConfig{Address: "1.1.1.1:11"},
				ServerConfig{Address: "2.2.2.2:22"},
			},
		},
	},
}

var invalidTlogStorageConfigsDeduped = append(invalidTlogStorageConfigs,
	// missing metadata storage server in given slave cluster
	TlogStorageConfig{
		ZeroStorCluster: ZeroStorClusterConfig{
			IYO: IYOCredentials{
				Org:       "foo org",
				Namespace: "foo namespace",
				ClientID:  "foo client",
				Secret:    "foo secret",
			},
			Servers: []ServerConfig{
				ServerConfig{Address: "1.1.1.1:11"},
				ServerConfig{Address: "2.2.2.2:22"},
			},
			MetadataServers: []ServerConfig{
				ServerConfig{Address: "3.3.3.3:33"},
			},
		},
		SlaveStorageCluster: &StorageClusterConfig{
			Servers: []StorageServerConfig{
				StorageServerConfig{
					Address: "localhost:16379",
				},
			},
		},
	})
