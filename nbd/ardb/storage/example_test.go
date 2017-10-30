package storage_test

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
)

func ExampleNewBlockStorage() {
	cluster := cluster()

	const (
		vdiskID   = "vdisk1"
		vdiskType = config.VdiskTypeDB
		blockSize = int64(4096)
	)

	blockStorage, err := storage.NewBlockStorage(
		storage.BlockStorageConfig{
			VdiskID:   vdiskID,
			VdiskType: vdiskType,
			BlockSize: blockSize,
		},
		cluster,
		nil,
	)
	panicOnError(err)
	defer blockStorage.Close()

	// set a block
	err = blockStorage.SetBlock(0, []byte{1, 2, 3, 4})
	panicOnError(err)

	// fetch the block we just set
	content, err := blockStorage.GetBlock(0)
	panicOnError(err)

	fmt.Print(content)
	// Output:
	// [1 2 3 4]
}

func ExampleNewBlockStorage_withTemplateCluster() {
	cluster, templateCluster := cluster(), cluster()

	const (
		vdiskID         = "vdisk1"
		templateVdiskID = "templateVdisk1"
		vdiskType       = config.VdiskTypeBoot
		blockSize       = int64(4096)
	)

	blockStorage, err := storage.NewBlockStorage(
		storage.BlockStorageConfig{
			VdiskID:   vdiskID,
			VdiskType: vdiskType,
			BlockSize: blockSize,
		},
		cluster,
		templateCluster,
	)
	panicOnError(err)
	defer blockStorage.Close()

	templateStorage, err := storage.NewBlockStorage(
		storage.BlockStorageConfig{
			VdiskID:   templateVdiskID,
			VdiskType: vdiskType,
			BlockSize: blockSize,
		},
		templateCluster,
		nil,
	)
	panicOnError(err)

	// set to template cluster
	err = templateStorage.SetBlock(0, []byte{1, 2, 3, 4})
	panicOnError(err)
	err = templateStorage.Flush()
	panicOnError(err)

	// copy (meta)data from template to storage cluster
	copyVdisk(templateVdiskID, vdiskID, templateCluster, cluster)

	// get from storage cluster
	content, err := blockStorage.GetBlock(0)
	panicOnError(err)

	fmt.Print(content)
	// Output:
	// [1 2 3 4]
}

func cluster() ardb.StorageCluster {
	return redisstub.NewUniCluster(false)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// simplified algorithm based on `zeroctl/cmd/copyvdisk/vdisk.go copyVdisk`
func copyVdisk(vdiskIDA, vdiskIDB string, clusterA, clusterB ardb.StorageCluster) {
	const (
		vtype     = config.VdiskTypeBoot
		blockSize = int64(4096)
	)
	err := storage.CopyVdisk(
		storage.CopyVdiskConfig{
			VdiskID:   vdiskIDA,
			Type:      vtype,
			BlockSize: blockSize,
		},
		storage.CopyVdiskConfig{
			VdiskID:   vdiskIDB,
			Type:      vtype,
			BlockSize: blockSize,
		},
		clusterA,
		clusterB,
	)
	panicOnError(err)
}
