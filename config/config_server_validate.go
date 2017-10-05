package config

import (
	"errors"

	"github.com/zero-os/0-Disk/log"
)

// ValidateNBDServerConfigs validates all available NBD Vdisk Configurations,
// for a given NBD server config, using a given config source.
func ValidateNBDServerConfigs(source Source, serverID string) error {
	cfg, err := ReadNBDVdisksConfig(source, serverID)
	if err != nil {
		return err
	}

	var errs validateErrors
	for _, vdiskID := range cfg.Vdisks {
		_, err = ReadNBDStorageConfig(source, vdiskID)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
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
	var errs validateErrors

	for _, vdiskID := range cfg.Vdisks {
		vdiskStaticConfig, err = ReadVdiskStaticConfig(source, vdiskID)
		if err != nil {
			errs = append(errs, err)
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
			errs = append(errs, err)
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
			errs = append(errs, err)
			continue
		}

		validTlogConfiguredVdiskCount++
	}

	if len(errs) > 0 {
		return errs
	}

	if validTlogConfiguredVdiskCount == 0 {
		return errors.New(
			"there is no vdisk that has tlog configuration, while at least one is required")
	}

	return nil
}

type validateErrors []error

func (errs validateErrors) Error() string {
	if len(errs) == 0 {
		return ""
	}

	var str string
	for _, err := range errs {
		str += err.Error() + ";"
	}
	return str
}
