package config

import (
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
)

// ValidateNBDServerConfigs validates all available NBD Vdisk Configurations,
// for a given NBD server config, using a given config source.
func ValidateNBDServerConfigs(source Source, serverID string) error {
	cfg, err := ReadNBDVdisksConfig(source, serverID)
	if err != nil {
		return err
	}

	errs := errors.NewErrorSlice()
	for _, vdiskID := range cfg.Vdisks {
		_, err = ReadNBDStorageConfig(source, vdiskID)
		if err != nil {
			errs.Add(err)
		}
	}

	return errs.AsError()
}

// ValidateTlogServerConfigs validates all available Tlog Vdisk Configurations,
// for a given Tlog server config, using a given config source.
func ValidateTlogServerConfigs(source Source, serverID string) error {
	cfg, err := ReadNBDVdisksConfig(source, serverID)
	if err != nil {
		return err
	}

	var vdiskStaticConfig *VdiskStaticConfig
	var nbdVdiskConfig *VdiskNBDConfig

	var validTlogConfiguredVdiskCount int
	errs := errors.NewErrorSlice()

	for _, vdiskID := range cfg.Vdisks {
		vdiskStaticConfig, err = ReadVdiskStaticConfig(source, vdiskID)
		if err != nil {
			errs.Add(err)
			continue
		}
		if !vdiskStaticConfig.Type.TlogSupport() {
			log.Debugf(
				"not validating tlog storage for vdisk %s as it has no tlog support",
				vdiskID)
			continue // no tlog support
		}
		nbdVdiskConfig, err = ReadVdiskNBDConfig(source, vdiskID)
		if err != nil {
			errs.Add(err)
			continue
		}
		if nbdVdiskConfig.TlogServerClusterID == "" {
			log.Debugf(
				"not validating tlog storage for vdisk %s as it has no tlog configured",
				vdiskID)
			continue // vdisk tlog support, but no tlog configured
		}

		// vdisk has tlog support, and configured tlog stuff
		// now let's try to read the tlog storage
		_, err = ReadTlogStorageConfig(source, vdiskID)
		if err != nil {
			errs.Add(err)
			continue
		}

		validTlogConfiguredVdiskCount++
	}

	if validTlogConfiguredVdiskCount == 0 {
		errs.Add(errors.New(
			"there is no vdisk that has tlog configuration, while at least one is required",
		))
	}

	return errs.AsError()
}
