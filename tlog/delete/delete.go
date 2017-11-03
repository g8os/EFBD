package delete

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/stor"
)

// Delete deletes tlog data of a vdisk
func Delete(confSource config.Source, vdiskID, privKey string) error {
	hasTlog, err := tlog.HasTlogCluster(confSource, vdiskID)
	if err != nil {
		return fmt.Errorf("failed to read config for vdisk `%v`: %v", vdiskID, err)
	}

	if !hasTlog {
		return nil
	}

	storCli, err := stor.NewClientFromConfigSource(confSource, vdiskID, privKey)
	if err != nil {
		return fmt.Errorf("failed to create 0-stor client for vdisk `%v`: %v", vdiskID, err)
	}

	return storCli.Delete()
}
