package main

import (
	"errors"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
)

// NewExportController creates a new export config manager.
func NewExportController(configInfo zerodisk.ConfigInfo, tlsOnly bool) (*ExportController, error) {
	if err := configInfo.Validate(); err != nil {
		return nil, errors.New(
			"ExportController requires a valid config resource: " + err.Error())
	}

	return &ExportController{
		configInfo: configInfo,
		tlsOnly:    tlsOnly,
	}, nil
}

// ExportController implements nbd.ExportConfigManager
// reading the BaseConfig when a config is required
type ExportController struct {
	configInfo zerodisk.ConfigInfo
	tlsOnly    bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() (exports []string) {
	cfg, err := zerodisk.ReadVdisksConfig(c.configInfo)
	if err != nil {
		log.Error(err)
		return
	}

	exports = cfg.List
	return
}

// GetConfig implements nbd.ExportConfigManager.GetConfig
func (c *ExportController) GetConfig(name string) (*nbd.ExportConfig, error) {
	log.Infof("Getting vdisk %q", name)

	cfg, err := zerodisk.ReadBaseConfig(name, c.configInfo)
	if err != nil {
		return nil, err
	}

	return &nbd.ExportConfig{
		Name:               name,
		Description:        "Deduped g8os zerodisk",
		Driver:             "ardb",
		ReadOnly:           cfg.ReadOnly,
		TLSOnly:            c.tlsOnly,
		MinimumBlockSize:   0, // use size given by ArdbBackend.Geometry
		PreferredBlockSize: 0, // use size given by ArdbBackend.Geometry
		MaximumBlockSize:   0, // use size given by ArdbBackend.Geometry
		// TODO: add ability to have custom DriverParameters
		// Related to following go-raml issues:
		//	+ https://github.com/Jumpscale/go-raml/issues/132
		//	+ https://github.com/Jumpscale/go-raml/issues/96
		// They are related in a way that we would need a way to have
		// a map[string[string] object generated
	}, nil
}
