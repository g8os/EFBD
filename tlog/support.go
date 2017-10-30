package tlog

import (
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
)

// HasTlogCluster returns true if the given vdiskID has tlog clusters
func HasTlogCluster(confSource config.Source, vdiskID string) (bool, error) {
	staticConf, err := config.ReadVdiskStaticConfig(confSource, vdiskID)
	if err != nil {
		return false, errors.Wrapf(err,
			"couldn't read vdisk %s's static config", vdiskID)
	}

	if !staticConf.Type.TlogSupport() {
		return false, nil
	}

	nbdConf, err := config.ReadVdiskNBDConfig(confSource, vdiskID)
	if err != nil {
		return false, errors.Wrapf(err,
			"couldn't read vdisk %s's NBD vdisk config", vdiskID)
	}

	return nbdConf.TlogServerClusterID != "", nil
}
