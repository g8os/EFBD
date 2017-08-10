package config

import (
	"context"

	"github.com/zero-os/0-Disk/log"
)

// ReadNBDVdisksConfig returns the requested NBDVdisksConfig
// from a given config source.
func ReadNBDVdisksConfig(source Source, serverID string) (*NBDVdisksConfig, error) {
	bytes, err := ReadConfig(source, serverID, KeyNBDServerVdisks)
	if err != nil {
		return nil, err
	}

	cfg, err := NewNBDVdisksConfig(bytes)
	if err != nil {
		source.MarkInvalidKey(Key{ID: serverID, Type: KeyNBDServerVdisks}, "")
		return nil, err
	}

	return cfg, nil
}

// ReadVdiskStaticConfig returns the requested VdiskStaticConfig
// from a given config source.
func ReadVdiskStaticConfig(source Source, vdiskID string) (*VdiskStaticConfig, error) {
	bytes, err := ReadConfig(source, vdiskID, KeyVdiskStatic)
	if err != nil {
		return nil, err
	}

	cfg, err := NewVdiskStaticConfig(bytes)
	if err != nil {
		source.MarkInvalidKey(Key{ID: vdiskID, Type: KeyVdiskStatic}, "")
		return nil, err
	}

	return cfg, nil
}

// ReadVdiskNBDConfig returns the requested VdiskNBDConfig
// from a given config source.
func ReadVdiskNBDConfig(source Source, vdiskID string) (*VdiskNBDConfig, error) {
	bytes, err := ReadConfig(source, vdiskID, KeyVdiskNBD)
	if err != nil {
		return nil, err
	}

	cfg, err := NewVdiskNBDConfig(bytes)
	if err != nil {
		source.MarkInvalidKey(Key{ID: vdiskID, Type: KeyVdiskNBD}, "")
		return nil, err
	}

	return cfg, nil
}

// ReadVdiskTlogConfig returns the requested VdiskTlogConfig
// from a given config source.
func ReadVdiskTlogConfig(source Source, vdiskID string) (*VdiskTlogConfig, error) {
	bytes, err := ReadConfig(source, vdiskID, KeyVdiskTlog)
	if err != nil {
		return nil, err
	}

	cfg, err := NewVdiskTlogConfig(bytes)
	if err != nil {
		source.MarkInvalidKey(Key{ID: vdiskID, Type: KeyVdiskTlog}, "")
		return nil, err
	}

	return cfg, nil
}

// ReadStorageClusterConfig returns the requested StorageClusterConfig
// from a given config source.
func ReadStorageClusterConfig(source Source, clusterID string) (*StorageClusterConfig, error) {
	bytes, err := ReadConfig(source, clusterID, KeyClusterStorage)
	if err != nil {
		return nil, err
	}

	cfg, err := NewStorageClusterConfig(bytes)
	if err != nil {
		source.MarkInvalidKey(Key{ID: clusterID, Type: KeyClusterStorage}, "")
		return nil, err
	}

	return cfg, nil
}

// ReadTlogClusterConfig returns the requested TlogClusterConfig
// from a given config source.
func ReadTlogClusterConfig(source Source, clusterID string) (*TlogClusterConfig, error) {
	bytes, err := ReadConfig(source, clusterID, KeyClusterTlog)
	if err != nil {
		return nil, err
	}

	cfg, err := NewTlogClusterConfig(bytes)
	if err != nil {
		source.MarkInvalidKey(Key{ID: clusterID, Type: KeyClusterTlog}, "")
		return nil, err
	}

	return cfg, nil
}

// ReadConfig returns the requested config as a byte slice from the given source.
func ReadConfig(source Source, id string, keyType KeyType) ([]byte, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	if id == "" {
		return nil, ErrNilID
	}

	configKey := Key{ID: id, Type: keyType}
	bytes, err := source.Get(configKey)
	if err == nil {
		return bytes, nil // config read successfully
	}

	if err == ErrConfigUnavailable {
		source.MarkInvalidKey(configKey, "")
	} else if err == ErrSourceUnavailable {
		log.Errorf("couldn't fetch config %v: %v", configKey, err)
		if source.Type() == "etcd" {
			log.Broadcast(
				log.StatusClusterTimeout,
				log.SubjectETCD,
				source.SourceConfig(),
			)
		}
	} else if _, isInvalid := err.(*InvalidConfigError); isInvalid {
		source.MarkInvalidKey(configKey, "")
	}

	return nil, err // config couldn't be read due to an error
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
		log.Debugf("Could not fetch initial config of NBDVdisksConfig watcher: %s", err)
		return nil, err
	}

	// setup channel
	updater := make(chan NBDVdisksConfig, 1)
	updater <- *cfg

	ctx = watchContext(ctx)
	configKey := Key{ID: serverID, Type: KeyNBDServerVdisks}
	bytesCh, err := source.Watch(ctx, configKey)
	if err != nil {
		log.Debugf("Could not create NBDVdisksConfig watcher: %s", err)
		return nil, err
	}

	go func() {
		log.Debugf("WatchNBDVdisksConfig for server %s started", serverID)
		defer close(updater)
		defer log.Debugf("WatchNBDVdisksConfig server for %s stopped", serverID)

		for {
			select {
			case <-ctx.Done():
				return

			case bytes, ok := <-bytesCh:
				if !ok {
					log.Debugf(
						"WatchNBDVdisksConfig for %v aborting due to closed input ch",
						serverID)
					return
				}

				cfg, err := NewNBDVdisksConfig(bytes)
				if err != nil {
					log.Debugf(
						"WatchNBDVdisksConfig for %v handled error: %v",
						serverID, err)
					source.MarkInvalidKey(configKey, "")
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %v",
						serverID)
					return
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
		log.Debugf("Could not fetch initial config for VdiskNBDConfig watcher: %s", err)
		return nil, err
	}

	// setup channel and send initial config value
	updater := make(chan VdiskNBDConfig, 1)
	updater <- *cfg

	ctx = watchContext(ctx)
	configKey := Key{ID: vdiskID, Type: KeyVdiskNBD}
	inputCh, err := source.Watch(ctx, configKey)
	if err != nil {
		log.Debugf("Could not create VdiskNBDConfig watcher: %s", err)
		return nil, err
	}

	go func() {
		log.Debugf("watchVdiskNBDConfig for vdisk %s started", vdiskID)
		defer close(updater)
		defer log.Debugf("watchVdiskNBDConfig for vdisk %s stopped", vdiskID)

		for {
			select {
			case <-ctx.Done():
				return

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
					source.MarkInvalidKey(configKey, "")
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %s",
						vdiskID)
					return
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
		log.Debugf("Could not fetch initial config for VdiskTlogConfig watcher: %s", err)
		return nil, err
	}

	// setup channel and send initial config value
	updater := make(chan VdiskTlogConfig, 1)
	updater <- *cfg

	ctx = watchContext(ctx)
	configKey := Key{ID: vdiskID, Type: KeyVdiskTlog}
	inputCh, err := source.Watch(ctx, configKey)
	if err != nil {
		log.Debugf("Could not create VdiskTlogConfig watcher: %s", err)
		return nil, err
	}

	go func() {
		log.Debugf("WatchVdiskTlogConfig for vdisk %s started", vdiskID)
		defer close(updater)
		defer log.Debugf("WatchVdiskTlogConfig for vdisk %s stopped", vdiskID)

		for {
			select {
			case <-ctx.Done():
				return

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
					source.MarkInvalidKey(configKey, "")
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %s",
						vdiskID)
					return
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
		log.Debugf("Could not fetch initial config for StorageClusterConfig watcher: %s", err)
		return nil, err
	}

	// setup channel and send initial config value
	updater := make(chan StorageClusterConfig, 1)
	updater <- *cfg

	ctx = watchContext(ctx)
	configKey := Key{ID: clusterID, Type: KeyClusterStorage}
	inputCh, err := source.Watch(ctx, configKey)
	if err != nil {
		log.Debugf("Could not create StorageClusterConfig watcher: %s", err)
		return nil, err
	}

	go func() {
		log.Debugf("WatchStorageClusterConfig for cluster %s started", clusterID)
		defer close(updater)
		defer log.Debugf("WatchStorageClusterConfig cluster for %s stopped", clusterID)

		for {
			select {
			case <-ctx.Done():
				return

			case bytes, ok := <-inputCh:
				if !ok {
					log.Debugf(
						"WatchStorageClusterConfig for %s aborting due to closed input ch",
						clusterID)
					return
				}
				log.Debugf(
					"WatchStorageClusterConfig for %s received config bytes from source",
					clusterID)

				cfg, err := NewStorageClusterConfig(bytes)
				if err != nil {
					source.MarkInvalidKey(configKey, "")
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %v",
						clusterID)
					return
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
		log.Debugf("Could not fetch initial config for TlogClusterConfig watcher: %s", err)
		return nil, err
	}

	// setup channel and send initial config value
	updater := make(chan TlogClusterConfig, 1)
	updater <- *cfg

	ctx = watchContext(ctx)
	configKey := Key{ID: clusterID, Type: KeyClusterTlog}
	inputCh, err := source.Watch(ctx, configKey)
	if err != nil {
		log.Debugf("Could not create TlogClusterConfig watcher: %s", err)
		return nil, err
	}

	go func() {
		log.Debugf("WatchTlogClusterConfig for cluster %s started", clusterID)
		defer close(updater)
		defer log.Debugf("WatchTlogClusterConfig cluster for %s stopped", clusterID)

		for {
			select {
			case <-ctx.Done():
				return

			case bytes, ok := <-inputCh:
				if !ok {
					log.Debugf(
						"WatchTlogClusterConfig for %s aborting due to closed input ch",
						clusterID)
					return
				}
				log.Debugf(
					"WatchTlogClusterConfig for %s received config bytes from source",
					clusterID)

				cfg, err := NewTlogClusterConfig(bytes)
				if err != nil {
					source.MarkInvalidKey(configKey, "")
					continue
				}

				select {
				case updater <- *cfg:
				// ensure we can't get stuck in a deadlock for this goroutine
				case <-ctx.Done():
					log.Errorf("timed out (ctx) while sending update for %s",
						clusterID)
					return
				}
			}
		}
	}()

	return updater, nil
}
