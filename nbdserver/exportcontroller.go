package main

import (
	"errors"
	"fmt"

	gridapi "github.com/g8os/blockstor/gridapi/gridapiclient"
	"github.com/g8os/gonbdserver/nbd"
	log "github.com/glendc/go-mini-log"
)

// NewExportController creates a new export config manager.
func NewExportController(gridapiaddress string, tslOnly bool, exports []string) (controller *ExportController, err error) {
	if gridapiaddress == "" {
		err = errors.New("ExportController requires a non-empty gridapiaddress")
		return
	}

	controller = &ExportController{
		gridapi: gridapi.NewG8OSStatelessGRID(),
		exports: exports,
		tslOnly: tslOnly,
	}
	controller.gridapi.BaseURI = gridapiaddress
	return
}

// ExportController implements nbd.ExportConfigManager
// using the GridAPI stateless client internally
type ExportController struct {
	gridapi *gridapi.G8OSStatelessGRID
	exports []string
	tslOnly bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() []string {
	return c.exports
}

// GetConfig implements nbd.ExportConfigManager.GetConfig
func (c *ExportController) GetConfig(name string) (*nbd.ExportConfig, error) {
	log.Infof("Getting vdisk %q", name)
	vdiskInfo, _, err := c.gridapi.Vdisks.GetVdiskInfo(
		name, // vdiskID
		nil,  // headers
		nil,  // queryParams
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't get vdisk %s: %s", name, err)
	}

	return &nbd.ExportConfig{
		Name:               name,
		Description:        "Deduped g8os blockstor",
		Driver:             "ardb",
		ReadOnly:           vdiskInfo.ReadOnly,
		TLSOnly:            c.tslOnly,
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
