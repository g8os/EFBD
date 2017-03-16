package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/g8os/blockstor/nbdserver/clients/volumecontroller"
	"github.com/g8os/gonbdserver/nbd"
)

// NewExportController creates a new export config manager.
func NewExportController(volumecontrolleraddress string, defaultExport string, tslOnly bool) (controller *ExportController, err error) {
	if volumecontrolleraddress == "" {
		err = errors.New("ExportController requires a non-empty volumecontrolleraddress")
		return
	}

	controller = &ExportController{
		volumeController:        volumecontroller.NewVolumeController(),
		volumecontrolleraddress: volumecontrolleraddress,
		defaultExport:           defaultExport,
		tslOnly:                 tslOnly,
	}
	controller.volumeController.BaseURI = volumecontrolleraddress
	return
}

// ExportController implements nbd.ExportConfigManager
// using the VolumeController client internally
type ExportController struct {
	volumeController        *volumecontroller.VolumeController
	volumecontrolleraddress string
	defaultExport           string
	tslOnly                 bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() []string {
	if c.defaultExport == "" {
		return nil
	}

	// TODO: Come up with a better solution to support listing exports.
	return []string{c.defaultExport}
}

// GetConfig implements nbd.ExportConfigManager.GetConfig
func (c *ExportController) GetConfig(name string) (*nbd.ExportConfig, error) {
	log.Printf("[INFO] Getting volume %q", name)
	volumeInfo, _, err := c.volumeController.Volumes.GetVolumeInfo(
		name, // volumeID
		nil,  // headers
		nil,  // queryParams
	)
	if err != nil {
		return nil, fmt.Errorf("couldn't get volume %s: %s", name, err)
	}

	return &nbd.ExportConfig{
		Name:               name,
		Description:        "Deduped g8os blockstor",
		Driver:             volumeInfo.Driver,
		ReadOnly:           volumeInfo.ReadOnly,
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
