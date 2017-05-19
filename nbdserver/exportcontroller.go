package main

import (
	"errors"
	"fmt"

	"github.com/g8os/blockstor/config"
	"github.com/g8os/blockstor/gonbdserver/nbd"
	"github.com/g8os/blockstor/log"
)

// NewExportController creates a new export config manager.
func NewExportController(configPath string, tlsOnly bool) (controller *ExportController, err error) {
	if configPath == "" {
		err = errors.New("ExportController requires a non-empty config path")
		return
	}

	controller = &ExportController{
		configPath: configPath,
		tlsOnly:    tlsOnly,
	}
	return
}

// ExportController implements nbd.ExportConfigManager
// using the GridAPI stateless client internally
type ExportController struct {
	configPath string
	tlsOnly    bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() (exports []string) {
	cfg, err := config.ReadConfig(c.configPath)
	if err != nil {
		log.Info("couldn't read nbdserver config:", err)
		return
	}

	for export := range cfg.Vdisks {
		exports = append(exports, export)
	}
	return
}

// GetConfig implements nbd.ExportConfigManager.GetConfig
func (c *ExportController) GetConfig(name string) (*nbd.ExportConfig, error) {
	log.Infof("Getting vdisk %q", name)

	cfg, err := config.ReadConfig(c.configPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't read nbdserver config: %s", err.Error())
	}
	vdisk, ok := cfg.Vdisks[name]
	if !ok {
		return nil, fmt.Errorf("couldn't find a config for vdisk %s", name)
	}

	return &nbd.ExportConfig{
		Name:               name,
		Description:        "Deduped g8os blockstor",
		Driver:             "ardb",
		ReadOnly:           vdisk.ReadOnly,
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
