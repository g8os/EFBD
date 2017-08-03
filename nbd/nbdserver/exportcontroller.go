package main

import (
	"context"
	"sync"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/gonbdserver/nbd"
)

// NewExportController creates a new export config manager.
func NewExportController(ctx context.Context, configSource config.Source, tlsOnly bool, serverID string) (*ExportController, error) {
	exportController := &ExportController{
		configSource: configSource,
		tlsOnly:      tlsOnly,
		done:         make(chan struct{}),
	}

	err := exportController.spawnBackground(ctx, serverID)
	if err != nil {
		return nil, err
	}

	return exportController, nil
}

// ExportController implements nbd.ExportConfigManager
// reading the BaseConfig when a config is required
type ExportController struct {
	configSource config.Source

	vdisksConfig config.NBDVdisksConfig
	vdisksMux    sync.RWMutex
	done         chan struct{}

	tlsOnly bool
}

// ListConfigNames implements nbd.ExportConfigManager.ListConfigNames
func (c *ExportController) ListConfigNames() (exports []string) {
	c.vdisksMux.RLock()
	defer c.vdisksMux.RUnlock()

	exports = make([]string, len(c.vdisksConfig.Vdisks))
	copy(exports, c.vdisksConfig.Vdisks)
	return
}

// GetConfig implements nbd.ExportConfigManager.GetConfig
func (c *ExportController) GetConfig(name string) (*nbd.ExportConfig, error) {
	log.Infof("Getting vdisk %q", name)

	cfg, err := config.ReadVdiskStaticConfig(c.configSource, name)
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

func (c *ExportController) reloadVdisksConfig(cfg config.NBDVdisksConfig) {
	c.vdisksMux.Lock()
	defer c.vdisksMux.Unlock()
	c.vdisksConfig = cfg
}

func (c *ExportController) spawnBackground(ctx context.Context, serverID string) error {
	ch, err := config.WatchNBDVdisksConfig(ctx, c.configSource, serverID)
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
