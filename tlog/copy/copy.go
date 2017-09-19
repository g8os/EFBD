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
	JobCount      int    `validate:"nonzero,min=1"`
}

// Copy copies tlog data from source vdisk to target vdisk
// - target vdisk doesn't have tlog cluster : do nothing
// - target vdisk has tlog cluster but source vdisk dont: generate tlog data for target vdisk
// - target and source vdisk have same tlog cluster : copy metadata and add rerefence to 0-stor object
// - target and source vdisk use different tlog cluster : copy both metadata and data
func Copy(ctx context.Context, confSource config.Source, conf Config) error {
	targetStaticConf, err := config.ReadVdiskStaticConfig(confSource, conf.TargetVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read target vdisk %s's static config: %v", conf.TargetVdiskID, err)
	}

	// target vdisk has no tlog support
	// we can do nothing
	if !targetStaticConf.Type.TlogSupport() {
		return nil
	}

	targetVdiskNBDConfig, err := config.ReadVdiskNBDConfig(confSource, conf.TargetVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read target vdisk %s's storage config: %v", conf.TargetVdiskID, err)
	}

	// it has no tlog cluster
	// do nothing
	if targetVdiskNBDConfig.TlogServerClusterID == "" {
		return nil
	}

	if err := validator.Validate(conf); err != nil {
		return err
	}

	sourceStaticConf, err := config.ReadVdiskStaticConfig(confSource, conf.SourceVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read source vdisk %s's static config: %v", conf.SourceVdiskID, err)
	}

	sourceVdiskNBDConfig, err := config.ReadVdiskNBDConfig(confSource, conf.SourceVdiskID)
	if err != nil {
		return fmt.Errorf(
			"couldn't read source vdisk %s's storage config: %v", conf.SourceVdiskID, err)
	}
	// if source vdisk has no tlog, generate it
	if !sourceStaticConf.Type.TlogSupport() || sourceVdiskNBDConfig.TlogServerClusterID == "" {
		log.Infof("generating tlog data for vdisk `%v`", conf.TargetVdiskID)
		generator, err := newGenerator(confSource, conf)
		if err != nil {
			return err
		}
		return generator.GenerateFromStorage(ctx)
	}

	log.Infof("copying tlog data for vdisk `%v`", conf.TargetVdiskID)
	// copy tlog data
	copier, err := newCopier(confSource, conf)
	if err != nil {
		return err
	}
	return copier.Copy()
}
