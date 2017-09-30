package tlog

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
)

// HasTlogCluster returns true if the given vdiskID has tlog clusters
func HasTlogCluster(confSource config.Source, vdiskID string) (bool, error) {
	staticConf, err := config.ReadVdiskStaticConfig(confSource, vdiskID)
	if err != nil {
		return false, fmt.Errorf(
			"couldn't read vdisk %s's static config: %v", vdiskID, err)
	}

	if !staticConf.Type.TlogSupport() {
		return false, nil
	}

	nbdConf, err := config.ReadVdiskNBDConfig(confSource, vdiskID)
	if err != nil {
		return false, fmt.Errorf(
			"couldn't read vdisk %s's NBD vdisk config: %v", vdiskID, err)
	}

	return nbdConf.TlogServerClusterID != "", nil
}
