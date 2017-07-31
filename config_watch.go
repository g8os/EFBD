package zerodisk

import (
	"context"
	"fmt"

	"github.com/zero-os/0-Disk/config"
)

// WatchNBDVdisksConfig watches a given source for NBDVdisksConfig updates.
// Sends the initial config to the channel when created,
// as well as any future updated versions of that config,
// for as long as the given context allows it.
func WatchNBDVdisksConfig(ctx context.Context, info ConfigInfo, serverID string) (<-chan config.NBDVdisksConfigResult, error) {
	source, err := info.CreateSource()
	if err != nil {
		return nil, fmt.Errorf("WatchNBDVdisksConfig failed: %v", err)
	}

	return config.WatchNBDVdisksConfig(ctx, source, serverID)
}
