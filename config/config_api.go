package config

import (
	"context"
	"fmt"

	"github.com/zero-os/0-Disk/log"
)

// ReadNBDVdisksConfig returns the requested NBDVdisksConfig
// from a given config source.
func ReadNBDVdisksConfig(source Source, serverID string) (*NBDVdisksConfig, error) {
	bytes, err := ReadConfig(source, serverID, KeyNBDServerVdisks)
	if err != nil {
		return nil, err
	}

	return NewNBDVdisksConfig(bytes)
}

// ReadVdiskStaticConfig returns the requested VdiskStaticConfig
// from a given config source.
func ReadVdiskStaticConfig(source Source, vdiskID string) (*VdiskStaticConfig, error) {
	bytes, err := ReadConfig(source, vdiskID, KeyVdiskStatic)
	if err != nil {
		return nil, err
	}

	return NewVdiskStaticConfig(bytes)
}

// ReadVdiskNBDConfig returns the requested VdiskNBDConfig
// from a given config source.
func ReadVdiskNBDConfig(source Source, vdiskID string) (*VdiskNBDConfig, error) {
	bytes, err := ReadConfig(source, vdiskID, KeyVdiskNBD)
	if err != nil {
		return nil, err
	}

	return NewVdiskNBDConfig(bytes)
}

// ReadVdiskTlogConfig returns the requested VdiskTlogConfig
// from a given config source.
func ReadVdiskTlogConfig(source Source, vdiskID string) (*VdiskTlogConfig, error) {
	bytes, err := ReadConfig(source, vdiskID, KeyVdiskTlog)
	if err != nil {
		return nil, err
	}

	return NewVdiskTlogConfig(bytes)
}

// ReadStorageClusterConfig returns the requested StorageClusterConfig
// from a given config source.
func ReadStorageClusterConfig(source Source, clusterID string) (*StorageClusterConfig, error) {
	bytes, err := ReadConfig(source, clusterID, KeyClusterStorage)
	if err != nil {
		return nil, err
	}

	return NewStorageClusterConfig(bytes)
}

// ReadTlogClusterConfig returns the requested TlogClusterConfig
// from a given config source.
func ReadTlogClusterConfig(source Source, clusterID string) (*TlogClusterConfig, error) {
	bytes, err := ReadConfig(source, clusterID, KeyClusterTlog)
	if err != nil {
		return nil, err
	}

	return NewTlogClusterConfig(bytes)
}

// ReadConfig returns the requested config as a byte slice from the given source.
func ReadConfig(source Source, id string, keyType KeyType) ([]byte, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if id == "" {
		return nil, ErrNilID
	}

	return source.Get(Key{
		ID:   id,
		Type: keyType,
	})
}

// WatchNBDVdisksConfig watches a given source for NBDVdisksConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
// An error is returned in case the watcher couldn't be started.
func WatchNBDVdisksConfig(ctx context.Context, source Source, serverID string) (<-chan NBDVdisksConfig, error) {
	// fetch current data
	cfg, err := ReadNBDVdisksConfig(source, serverID)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of NBDVdisksConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan NBDVdisksConfig, 2)
	updater <- *cfg

	ctx = watchContext(ctx)
	bytesCh, err := source.Watch(ctx, Key{ID: serverID, Type: KeyNBDServerVdisks})
	if err != nil {
		return nil, fmt.Errorf("Could not create NBDVdisksConfig watcher: %s", err)
	}

	go func() {
		defer close(updater)

		for {
			select {
			case bytes, ok := <-bytesCh:
				if !ok {
					log.Debugf(
						"WatchNBDVdisksConfig for %v aborting due to closed input ch",
						serverID)
					return
				}

				cfg, err := NewNBDVdisksConfig(bytes)
				if err != nil {
					// TODO: Notify 0-Orchestrator
					log.Errorf(
						"WatchNBDVdisksConfig for %v received invalid config: %v",
						serverID, err)
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %v",
						serverID)
					continue
				}
			}
		}
	}()

	return updater, nil
}

// WatchVdiskNBDConfig watches a given source for VdiskNBDConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
// An error is returned in case the watcher couldn't be started.
func WatchVdiskNBDConfig(ctx context.Context, source Source, vdiskID string) (<-chan VdiskNBDConfig, error) {
	cfg, err := ReadVdiskNBDConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config for VdiskNBDConfig watcher: %s", err)
	}

	// setup channel and send initial config value
	updater := make(chan VdiskNBDConfig, 2)
	updater <- *cfg

	ctx = watchContext(ctx)
	inputCh, err := source.Watch(ctx, Key{ID: vdiskID, Type: KeyVdiskNBD})
	if err != nil {
		return nil, fmt.Errorf("Could not create VdiskNBDConfig watcher: %s", err)
	}

	go func() {
		defer close(updater)

		for {
			select {
			case bytes, ok := <-inputCh:
				if !ok {
					log.Debugf(
						"watchVdiskNBDConfig for %s aborting due to closed input ch",
						vdiskID)
					return
				}
				log.Debugf(
					"watchVdiskNBDConfig for %s received config bytes from source",
					vdiskID)

				cfg, err := NewVdiskNBDConfig(bytes)
				if err != nil {
					// TODO: Notify 0-Orchestrator
					log.Errorf(
						"watchVdiskNBDConfig for %s received invalid config: %v",
						vdiskID, err)
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %s",
						vdiskID)
					continue
				}
			}
		}
	}()

	return updater, nil
}

// WatchVdiskTlogConfig watches a given source for VdiskTlogConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
// An error is returned in case the watcher couldn't be started.
func WatchVdiskTlogConfig(ctx context.Context, source Source, vdiskID string) (<-chan VdiskTlogConfig, error) {
	cfg, err := ReadVdiskTlogConfig(source, vdiskID)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config for VdiskTlogConfig watcher: %s", err)
	}

	// setup channel and send initial config value
	updater := make(chan VdiskTlogConfig, 2)
	updater <- *cfg

	ctx = watchContext(ctx)
	inputCh, err := source.Watch(ctx, Key{ID: vdiskID, Type: KeyVdiskTlog})
	if err != nil {
		return nil, fmt.Errorf("Could not create VdiskTlogConfig watcher: %s", err)
	}

	go func() {
		defer close(updater)

		for {
			select {
			case bytes, ok := <-inputCh:
				if !ok {
					log.Debugf(
						"WatchVdiskTlogConfig for %s aborting due to closed input ch",
						vdiskID)
					return
				}
				log.Debugf(
					"WatchVdiskTlogConfig for %s received config bytes from source",
					vdiskID)

				cfg, err := NewVdiskTlogConfig(bytes)
				if err != nil {
					// TODO: Notify 0-Orchestrator
					log.Errorf(
						"WatchVdiskTlogConfig for %s received invalid config: %v",
						vdiskID, err)
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %s",
						vdiskID)
					continue
				}
			}
		}
	}()

	return updater, nil
}

// WatchStorageClusterConfig watches a given source for StorageClusterConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
// An error is returned in case the watcher couldn't be started.
func WatchStorageClusterConfig(ctx context.Context, source Source, clusterID string) (<-chan StorageClusterConfig, error) {
	cfg, err := ReadStorageClusterConfig(source, clusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config for StorageClusterConfig watcher: %s", err)
	}

	// setup channel and send initial config value
	updater := make(chan StorageClusterConfig, 2)
	updater <- *cfg

	ctx = watchContext(ctx)
	inputCh, err := source.Watch(ctx, Key{ID: clusterID, Type: KeyClusterStorage})
	if err != nil {
		return nil, fmt.Errorf("Could not create StorageClusterConfig watcher: %s", err)
	}

	go func() {
		defer close(updater)

		for {
			select {
			case bytes, ok := <-inputCh:
				if !ok {
					log.Debugf(
						"StorageClusterConfig for %s aborting due to closed input ch",
						clusterID)
					return
				}
				log.Debugf(
					"StorageClusterConfig for %s received config bytes from source",
					clusterID)

				cfg, err := NewStorageClusterConfig(bytes)
				if err != nil {
					// TODO: Notify 0-Orchestrator
					log.Errorf(
						"StorageClusterConfig for %s received invalid config: %v",
						clusterID, err)
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %v",
						clusterID)
					continue
				}
			}
		}
	}()

	return updater, nil
}

// WatchTlogClusterConfig watches a given source for TlogClusterConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
// An error is returned in case the watcher couldn't be started.
func WatchTlogClusterConfig(ctx context.Context, source Source, clusterID string) (<-chan TlogClusterConfig, error) {
	cfg, err := ReadTlogClusterConfig(source, clusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config for TlogClusterConfig watcher: %s", err)
	}

	// setup channel and send initial config value
	updater := make(chan TlogClusterConfig, 2)
	updater <- *cfg

	ctx = watchContext(ctx)
	inputCh, err := source.Watch(ctx, Key{ID: clusterID, Type: KeyClusterTlog})
	if err != nil {
		return nil, fmt.Errorf("Could not create TlogClusterConfig watcher: %s", err)
	}

	go func() {
		defer close(updater)

		for {
			select {
			case bytes, ok := <-inputCh:
				if !ok {
					log.Debugf(
						"TlogClusterConfig for %s aborting due to closed input ch",
						clusterID)
					return
				}
				log.Debugf(
					"TlogClusterConfig for %s received config bytes from source",
					clusterID)

				cfg, err := NewTlogClusterConfig(bytes)
				if err != nil {
					// TODO: Notify 0-Orchestrator
					log.Errorf(
						"TlogClusterConfig for %s received invalid config: %v",
						clusterID, err)
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %s",
						clusterID)
					continue
				}
			}
		}
	}()

	return updater, nil
}
