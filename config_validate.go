package zerodisk

// ValidateNBDServerConfigs validates all available NBD Vdisk Configurations,
// for a given NBD server config, using a given config resource.
func ValidateNBDServerConfigs(info ConfigInfo, serverID string) error {
	cfg, err := ReadNBDVdisksConfig(info, serverID)
	if err != nil {
		return err
	}

	var errs validateErrors
	for _, vdiskID := range cfg.Vdisks {
		_, err = ReadNBDStorageConfig(info, vdiskID)
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
// for a given Tlog server config, using a given config resource.
func ValidateTlogServerConfigs(info ConfigInfo, serverID string) error {
	cfg, err := ReadNBDVdisksConfig(info, serverID)
	if err != nil {
		return err
	}

	var errs validateErrors
	for _, vdiskID := range cfg.Vdisks {
		_, err = ReadTlogStorageConfig(info, vdiskID)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs
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
