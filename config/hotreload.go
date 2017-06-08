package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zero-os/0-Disk/log"
)

// HotReloader is used to subscribe channels,
// which will receive an up-to-date vdisk cluster config,
// each time a SIGHUP signal has been received by the HotReloader.
type HotReloader interface {
	// Subscribe allows you to get notified when a SIGHUP signal has been received,
	// upon which all registered channels receive the newest vdisk cluster config if possible.
	// Right upon calling this function, the given channel will already receive the last
	// available version taken from given configPath or cache.
	Subscribe(c chan<- VdiskClusterConfig, vdiskID string) error
	// Unsubscribe stops a channel from receiving the latest config for a vdisk
	// when receiving the SIGHUP signal.
	Unsubscribe(c chan<- VdiskClusterConfig) error

	// VdiskClusterConfig get the last loaded cluster config for a given VdiskID
	VdiskClusterConfig(vdiskID string) (*VdiskClusterConfig, error)
	// VdiskIdentifiers lists identifiers of the vdisks available
	// in the last loaded config
	VdiskIdentifiers() []string

	// Listen for incoming SIGHUP Signals,
	// upon which the tracked configs will be reloaded from disk,
	// and all channels will be given their new up-to-date vdisk cluster config.
	Listen(ctx context.Context)
	// Close the Listener background thread
	// and any other open resources
	Close() error
}

// NewHotReloader returns a HotReloader,
// which is used to subscribe channels,
// which will receive an up-to-date vdisk cluster config,
// each time a SIGHUP signal has been received by the HotReloader.
func NewHotReloader(configPath string, user User) (HotReloader, error) {
	cfg, err := ReadConfig(configPath, user)
	if err != nil {
		return nil, err
	}

	return &defaultHotReloader{
		handlers:     make(map[chan<- VdiskClusterConfig]string),
		cfgpath:      configPath,
		cfguser:      user,
		cfg:          cfg,
		fullReloadCh: make(chan struct{}),
		done:         make(chan struct{}),
	}, nil
}

type defaultHotReloader struct {
	handlers     map[chan<- VdiskClusterConfig]string
	cfgpath      string
	cfguser      User
	cfg          *Config
	fullReloadCh chan struct{}
	done         chan struct{}
	handlerMux   sync.Mutex
	mux          sync.RWMutex
}

// Subscribe implements HotReloader.defaultHotReloader
func (hr *defaultHotReloader) Subscribe(c chan<- VdiskClusterConfig, vdiskID string) error {
	if c == nil {
		return errors.New("zero-os/0-Disk: Subscribe using nil channel")
	}

	hr.handlerMux.Lock()
	defer hr.handlerMux.Unlock()

	log.Debugf("subscribe %v for vdisk %s", c, vdiskID)

	hr.handlers[c] = vdiskID
	return nil
}

// Unsubscribe implements HotReloader.Unsubscribe
func (hr *defaultHotReloader) Unsubscribe(c chan<- VdiskClusterConfig) error {
	if c == nil {
		return errors.New("zero-os/0-Disk: Unsubscribe using nil channel")
	}

	hr.handlerMux.Lock()
	defer hr.handlerMux.Unlock()

	log.Debugf("unsubscribe %v for vdisk %s", c, hr.handlers[c])

	delete(hr.handlers, c)
	return nil
}

// VdiskClusterConfig implements HotReloader.VdiskClusterConfig
func (hr *defaultHotReloader) VdiskClusterConfig(vdiskID string) (*VdiskClusterConfig, error) {
	hr.mux.RLock()
	defer hr.mux.RUnlock()
	return hr.cfg.VdiskClusterConfig(vdiskID)
}

// VdiskIdentifiers implements HotReloader.VdiskIdentifiers
func (hr *defaultHotReloader) VdiskIdentifiers() (ids []string) {
	hr.mux.RLock()
	defer hr.mux.RUnlock()

	for id := range hr.cfg.Vdisks {
		ids = append(ids, id)
	}
	return
}

// Listen implements HotReloader.Listen
func (hr *defaultHotReloader) Listen(ctx context.Context) {
	log.Info("ready to reload config(s) upon SIGHUP receival")
	go hr.reloader(ctx)
	hr.sighupListener(ctx)
}

// sighupListener listens to any incoming SIGHUP signals,
// and calls the reloader function in case such a signal has been received.
func (hr *defaultHotReloader) sighupListener(ctx context.Context) {
	log.Debug("sighupListener active")

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGHUP)

	defer func() {
		signal.Stop(ch)
		close(ch)
	}()

	for {
		select {
		case s := <-ch:
			switch s {
			case syscall.SIGHUP:
				hr.triggerFullReload()

			default:
				log.Error("received unsupported signal: ", s)
			}

		case <-hr.done:
			log.Debug("exiting HotReloader listener")
			return

		case <-ctx.Done():
			log.Debug("aborting HotReloader listener")
			return
		}
	}
}

// Close implements HotReloader.Close
func (hr *defaultHotReloader) Close() error {
	close(hr.done)
	return nil
}

// trigger the reload for all registered channels
func (hr *defaultHotReloader) triggerFullReload() {
	hr.fullReloadCh <- struct{}{}
}

// reloader background thread,
// such that no reload action can ever block the user
func (hr *defaultHotReloader) reloader(ctx context.Context) {
	log.Debug("defaultHotReloader reloader is active")

	for {
		select {
		case <-hr.fullReloadCh:
			func() {
				defer func() {
					// recover any panic, as to not shut down listener
					if r := recover(); r != nil {
						log.Error("panic in HotReloader.Listen handling: ", r)
					}
				}()

				if err := hr.reloadVdiskClusterConfigs(); err != nil {
					log.Error(err)
				}
				return
			}()

		case <-hr.done:
			log.Debug("exiting HotReloader reloader")
			return

		case <-ctx.Done():
			log.Debug("aborting HotReloader reloader")
			return
		}
	}
}

// reloadVdiskClusterConfigs first reloads all cached configs ONCE,
// and sends the specific vdisk cluster configs to each registered notifier
// using the freshly cached configs
func (hr *defaultHotReloader) reloadVdiskClusterConfigs() error {
	// load config
	err := func() error {
		hr.mux.Lock()
		defer hr.mux.Unlock()

		var err error
		hr.cfg, err = ReadConfig(hr.cfgpath, hr.cfguser)
		if err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return err
	}

	if len(hr.handlers) == 0 {
		log.Debug("reloadVdiskClusterConfigs exits early, no handlers registered")
		return nil // nothing to do
	}

	var errs reloadErrors

	hr.handlerMux.Lock()
	defer hr.handlerMux.Unlock()

	hr.mux.RLock()
	defer hr.mux.RUnlock()

	for c, vdiskID := range hr.handlers {
		err = hr.reloadVdiskClusterConfig(c, vdiskID, 100*time.Millisecond)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		err = errs
	}

	return err
}

// reloadVdiskClusterConfig reloads the vdisk cluster config for a given channel
func (hr *defaultHotReloader) reloadVdiskClusterConfig(c chan<- VdiskClusterConfig, vdiskID string, wait time.Duration) error {
	// get cluster config for vdiskID if possible
	clusterConfig, err := hr.cfg.VdiskClusterConfig(vdiskID)
	if err != nil {
		return err
	}

	select {
	case c <- *clusterConfig:
	case <-time.After(wait):
		return fmt.Errorf(
			"ReloadVdiskClusterConfig timed out for: %s (%s)",
			hr.cfgpath, vdiskID)
	}

	return nil
}

type reloadErrors []error

// Error implements error.Error
func (es reloadErrors) Error() string {
	var err string
	for _, e := range es {
		err += e.Error() + ";"
	}
	return err
}

// NopHotReloader returns a placeholder HotReloader,
// which doesn't hot reload at all, but simply returns a static configuration,
// usuable for places where a HotReloader is required,
// but where it doesn't make any sense.
func NopHotReloader(configPath string, user User) (HotReloader, error) {
	cfg, err := ReadConfig(configPath, user)
	if err != nil {
		return nil, fmt.Errorf("couldn't create NopHotReloader: %s", err.Error())
	}

	return &nopHotReloader{cfg}, nil
}

type nopHotReloader struct {
	cfg *Config
}

// Subscribe implements HotReloader.Subscribe
func (hr *nopHotReloader) Subscribe(c chan<- VdiskClusterConfig, vdiskID string) error { return nil }

// Unsubscribe implements HotReloader.Unsubscribe
func (hr *nopHotReloader) Unsubscribe(c chan<- VdiskClusterConfig) error { return nil }

// VdiskClusterConfig implements HotReloader.VdiskClusterConfig
func (hr *nopHotReloader) VdiskClusterConfig(vdiskID string) (*VdiskClusterConfig, error) {
	return hr.cfg.VdiskClusterConfig(vdiskID)
}

// VdiskIdentifiers implements HotReloader.VdiskIdentifiers
func (hr *nopHotReloader) VdiskIdentifiers() (ids []string) {
	for id := range hr.cfg.Vdisks {
		ids = append(ids, id)
	}
	return
}

// Listen implements HotReloader.Listen
func (hr *nopHotReloader) Listen(ctx context.Context) {}

// Close implements HotReloader.Close
func (hr *nopHotReloader) Close() error { return nil }
