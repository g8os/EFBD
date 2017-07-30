package main

import (
	"context"
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
)

// NewExportController creates a new export config manager.
func NewExportController(ctx context.Context, configInfo zerodisk.ConfigInfo, tlsOnly bool) (*ExportController, error) {
	exportController := &ExportController{
		configInfo: configInfo,
		tlsOnly:    tlsOnly,
		done:       make(chan struct{}),
	}

	err := exportController.spawnBackground(ctx, configInfo)
	if err != nil {
		return nil, err
	}

	return exportController, nil
}

// ExportController implements nbd.ExportConfigManager
// reading the BaseConfig when a config is required
type ExportController struct {
	configInfo zerodisk.ConfigInfo

	vdisksConfig config.VdisksConfig
	vdisksMux    sync.RWMutex
	done         chan struct{}

	tlsOnly bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() (exports []string) {
	c.vdisksMux.RLock()
	defer c.vdisksMux.RUnlock()

	exports = make([]string, len(c.vdisksConfig.List))
	copy(exports, c.vdisksConfig.List)
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

// Close any open resources.
func (c *ExportController) Close() error {
	if c.done == nil {
		return nil
	}

	close(c.done)
	return nil
}

func (c *ExportController) reloadVdisksConfig(cfg config.VdisksConfig) {
	c.vdisksMux.Lock()
	defer c.vdisksMux.Unlock()
	c.vdisksConfig = cfg
}

func (c *ExportController) spawnBackground(ctx context.Context, configInfo zerodisk.ConfigInfo) error {
	ch, err := zerodisk.WatchVdisksConfig(ctx, configInfo)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return nil
	case c.vdisksConfig = <-ch:
	}

	go func() {
		log.Debug("started ExportController background thread (config hot reloading)")
		for {
			select {
			case <-ctx.Done():
				log.Debug("abort ExportController background thread")
				return

			case <-c.done:
				log.Debug("exit ExportController background thread")
				return

			case cfg := <-ch:
				c.reloadVdisksConfig(cfg)
			}
		}
	}()

	return nil
}
