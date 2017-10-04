package copy

import (
	"context"
	"fmt"

	"gopkg.in/validator.v2"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
)

// Config represents config for copy operation
type Config struct {
	SourceVdiskID string `validate:"nonzero"`
	TargetVdiskID string `validate:"nonzero"`
	PrivKey       string `validate:"nonzero"`
	DataShards    int    `validate:"nonzero,min=1"`
	ParityShards  int    `validate:"nonzero,min=1"`
	FlushSize     int
	JobCount      int `validate:"nonzero,min=1"`
}

// Copy copies tlog data from source vdisk to target vdisk
// - target vdisk doesn't have tlog cluster : do nothing
// - target vdisk has tlog cluster but source vdisk dont: generate tlog data for target vdisk
// - target and source vdisk have same tlog cluster : copy metadata and add rerefence to 0-stor object
// - target and source vdisk use different tlog cluster : copy both metadata and data
func Copy(ctx context.Context, confSource config.Source, conf Config) error {
	targetStaticConf, err := config.ReadVdiskStaticConfig(confSource, conf.TargetVdiskID)
	if err != nil {
		if err == config.ErrConfigUnavailable {
			log.Infof(
				"won't copy/generate tlog data as no VdiskStaticConfig was found for target vdisk `%s`",
				conf.TargetVdiskID)
			return nil // nothing to do
		}
		return fmt.Errorf(
			"couldn't read target vdisk %s's static config: %v", conf.TargetVdiskID, err)
	}

	// target vdisk has no tlog support
	// we can do nothing
	if !targetStaticConf.Type.TlogSupport() {
		return nil
	}
	targetNBDConf, err := config.ReadVdiskNBDConfig(confSource, conf.TargetVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read target vdisk %s's NBD vdisk config: %v", conf.TargetVdiskID, err)
	}
	if targetNBDConf.TlogServerClusterID == "" {
		return nil // nothing to do, no target tlog server cluster defined
	}

	if err := validator.Validate(conf); err != nil {
		return err
	}

	sourceStaticConf, err := config.ReadVdiskStaticConfig(confSource, conf.SourceVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read source vdisk %s's static config: %v", conf.SourceVdiskID, err)
	}

	if sourceStaticConf.Type.TlogSupport() {
		sourceNBDConf, err := config.ReadVdiskNBDConfig(confSource, conf.SourceVdiskID)
		if err != nil {
			return fmt.Errorf(
				"couldn't read source vdisk %s's NBD vdisk config: %v", conf.SourceVdiskID, err)
		}
		if sourceNBDConf.TlogServerClusterID != "" {
			// copy tlog data
			copier, err := newCopier(confSource, conf)
			if err != nil {
				return err
			}
			return copier.Copy()
		}
	}

	// source vdisk has no tlog, generate it
	generator, err := NewGenerator(confSource, conf)
	if err != nil {
		return err
	}
	_, err = generator.GenerateFromStorage(ctx)
	return err
}
