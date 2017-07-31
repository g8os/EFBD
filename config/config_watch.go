package config

import (
	"context"
	"fmt"
)

// WatchNBDVdisksConfig watches a given source for NBDVdisksConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchNBDVdisksConfig(ctx context.Context, source Source, serverID string) (<-chan NBDVdisksConfigResult, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	ctx = watchContext(ctx)

	key := Key{
		ID:   serverID,
		Type: KeyNBDServerVdisks,
	}

	// fetch current data
	bytes, err := source.Get(key)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of NBDVdisksConfig watcher: %s", err)
	}
	cfg, err := NewNBDVdisksConfig(bytes)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of NBDVdisksConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan NBDVdisksConfigResult, 1)
	updater <- NBDVdisksConfigResult{Value: cfg}

	err = source.Watch(ctx, key, func(bytes []byte) error {
		var result NBDVdisksConfigResult
		result.Value, result.Error = NewNBDVdisksConfig(bytes)

		// send current data (or error) to channel
		select {
		case updater <- result:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return result.Error
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create NBDVdisksConfig watcher: %s", err)
	}

	return updater, nil
}

// WatchVdiskNBDConfig watches a given source for VdiskNBDConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchVdiskNBDConfig(ctx context.Context, source Source, vdiskID string) (<-chan VdiskNBDConfigResult, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	ctx = watchContext(ctx)

	key := Key{
		ID:   vdiskID,
		Type: KeyVdiskNBD,
	}

	// fetch current data
	bytes, err := source.Get(key)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of VdiskNBDConfig watcher: %s", err)
	}
	cfg, err := NewVdiskNBDConfig(bytes)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of VdiskNBDConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan VdiskNBDConfigResult, 1)
	updater <- VdiskNBDConfigResult{Value: cfg}

	err = source.Watch(ctx, key, func(bytes []byte) error {
		var result VdiskNBDConfigResult
		result.Value, result.Error = NewVdiskNBDConfig(bytes)

		// send current data (or error) to channel
		select {
		case updater <- result:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return result.Error
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create VdiskNBDConfig watcher: %s", err)
	}

	return updater, nil
}

// WatchVdiskTlogConfig watches a given source for VdiskNBDConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchVdiskTlogConfig(ctx context.Context, source Source, vdiskID string) (<-chan VdiskTlogConfigResult, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	ctx = watchContext(ctx)

	key := Key{
		ID:   vdiskID,
		Type: KeyVdiskTlog,
	}

	// fetch current data
	bytes, err := source.Get(key)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of VdiskTlogConfig watcher: %s", err)
	}
	cfg, err := NewVdiskTlogConfig(bytes)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of VdiskTlogConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan VdiskTlogConfigResult, 1)
	updater <- VdiskTlogConfigResult{Value: cfg}

	err = source.Watch(ctx, key, func(bytes []byte) error {
		var result VdiskTlogConfigResult
		result.Value, result.Error = NewVdiskTlogConfig(bytes)

		// send current data (or error) to channel
		select {
		case updater <- result:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create VdiskTlogConfig watcher: %s", err)
	}

	return updater, nil
}

// WatchStorageClusterConfig watches a given source for StorageClusterConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchStorageClusterConfig(ctx context.Context, source Source, vdiskID string) (<-chan StorageClusterConfigResult, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	ctx = watchContext(ctx)

	key := Key{
		ID:   vdiskID,
		Type: KeyClusterStorage,
	}

	// fetch current data
	bytes, err := source.Get(key)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of StorageClusterConfig watcher: %s", err)
	}
	cfg, err := NewStorageClusterConfig(bytes)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of StorageClusterConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan StorageClusterConfigResult, 1)
	updater <- StorageClusterConfigResult{Value: cfg}

	err = source.Watch(ctx, key, func(bytes []byte) error {
		var result StorageClusterConfigResult
		result.Value, result.Error = NewStorageClusterConfig(bytes)

		// send current data (or error) to channel
		select {
		case updater <- result:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return result.Error
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create StorageClusterConfig watcher: %s", err)
	}

	return updater, nil
}

// WatchTlogClusterConfig watches a given source for TlogClusterConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchTlogClusterConfig(ctx context.Context, source Source, clusterID string) (<-chan TlogClusterConfigResult, error) {
	if source == nil {
		return nil, ErrNilSource
	}
	ctx = watchContext(ctx)

	key := Key{
		ID:   clusterID,
		Type: KeyClusterTlog,
	}

	// fetch current data
	bytes, err := source.Get(key)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of TlogClusterConfig watcher: %s", err)
	}
	cfg, err := NewTlogClusterConfig(bytes)
	if err != nil {
		return nil, fmt.Errorf(
			"Could not fetch initial config of TlogClusterConfig watcher: %s", err)
	}

	// setup channel
	updater := make(chan TlogClusterConfigResult, 1)
	updater <- TlogClusterConfigResult{Value: cfg}

	err = source.Watch(ctx, key, func(bytes []byte) error {
		var result TlogClusterConfigResult
		result.Value, result.Error = NewTlogClusterConfig(bytes)

		// send current data (or error) to channel
		select {
		case updater <- result:
		// ensure we can't get stuck in a deadlock for this goroutine
		case <-ctx.Done():
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("Could not create TlogClusterConfig watcher: %s", err)
	}

	return updater, nil
}

// NBDVdisksConfigResult is the result returned over
// a channel when watching a NBDVdisksConfig.
type NBDVdisksConfigResult struct {
	Value *NBDVdisksConfig
	Error error
}

// VdiskNBDConfigResult is the result returned over
// a channel when watching a VdiskNBDConfig.
type VdiskNBDConfigResult struct {
	Value *VdiskNBDConfig
	Error error
}

// VdiskTlogConfigResult is the result returned over
// a channel when watching a VdiskTlogConfig.
type VdiskTlogConfigResult struct {
	Value *VdiskTlogConfig
	Error error
}

// StorageClusterConfigResult is the result returned over
// a channel when watching a StorageClusterConfig.
type StorageClusterConfigResult struct {
	Value *StorageClusterConfig
	Error error
}

// TlogClusterConfigResult is the result returned over
// a channel when watching a TlogClusterConfig.
type TlogClusterConfigResult struct {
	Value *TlogClusterConfig
	Error error
}
