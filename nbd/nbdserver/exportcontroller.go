package main

import (
	"errors"
	"fmt"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
)

// NewExportController creates a new export config manager.
func NewExportController(cfg config.HotReloader, tlsOnly bool) (controller *ExportController, err error) {
	if cfg == nil {
		err = errors.New("ExportController requires a non-nil config.HotReloader")
		return
	}

	controller = &ExportController{
		cfg:     cfg,
		tlsOnly: tlsOnly,
	}
	return
}

// ExportController implements nbd.ExportConfigManager
// using the NBDServer HotReloader internally.
type ExportController struct {
	cfg     config.HotReloader
	tlsOnly bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() (exports []string) {
	exports = c.cfg.VdiskIdentifiers()
	return
}

// GetConfig implements nbd.ExportConfigManager.GetConfig
func (c *ExportController) GetConfig(name string) (*nbd.ExportConfig, error) {
	log.Infof("Getting vdisk %q", name)

	cfg, err := c.cfg.VdiskClusterConfig(name)
	if err != nil {
		return nil, fmt.Errorf("couldn't read zerodisk config: %s", err.Error())
	}

	return &nbd.ExportConfig{
		Name:               name,
		Description:        "Deduped g8os zerodisk",
		Driver:             "ardb",
		ReadOnly:           cfg.Vdisk.ReadOnly,
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
