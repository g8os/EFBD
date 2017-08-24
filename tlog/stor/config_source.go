package stor

import (
	"github.com/zero-os/0-Disk/config"
)

func ConfigFromConfigSource(source config.Source, vdiskID, privKey string, dataShards, parityShards int) (conf Config, err error) {
	// read vdisk config
	vdiskConf, err := config.ReadVdiskTlogConfig(source, vdiskID)
	if err != nil {
		return
	}

	// read zerostor config of this vdisk
	zsc, err := config.ReadZeroStoreClusterConfig(source, vdiskConf.ZeroStorClusterID)
	if err != nil {
		return
	}

	// creates stor config
	serverAddrs := func() (addrs []string) {
		for _, s := range zsc.Servers {
			addrs = append(addrs, s.Address)
		}
		return
	}()

	metaServerAddrs := func() (addrs []string) {
		for _, s := range zsc.MetadataServers {
			addrs = append(addrs, s.Address)
		}
		return
	}()

	return Config{
		VdiskID:         vdiskID,
		Organization:    zsc.IYO.Org,
		Namespace:       zsc.IYO.Namespace,
		IyoClientID:     zsc.IYO.ClientID,
		IyoSecret:       zsc.IYO.Secret,
		ZeroStorShards:  serverAddrs,
		MetaShards:      metaServerAddrs,
		DataShardsNum:   dataShards,
		ParityShardsNum: parityShards,
		EncryptPrivKey:  privKey,
	}, nil
}
