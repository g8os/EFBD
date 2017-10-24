package storage_test

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
	"github.com/zero-os/0-Disk/redisstub"
)

func ExampleNewBlockStorage() {
	cluster := cluster()

	vdiskID := "vdisk1"
	vdiskType := config.VdiskTypeDB
	blockSize := int64(4096)

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

	vdiskID := "vdisk1"
	templateVdiskID := "templateVdisk1"
	vdiskType := config.VdiskTypeBoot
	blockSize := int64(4096)

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

	templateStorage, err := storage.Deduped(
		templateVdiskID,
		blockSize,
		ardb.DefaultLBACacheLimit,
		templateCluster,
		nil,
	)

	// set to template cluster
	err = templateStorage.SetBlock(0, []byte{1, 2, 3, 4})
	panicOnError(err)
	err = templateStorage.Flush()
	panicOnError(err)

	// copy (meta)data from template to storage
	copyVdisk(templateVdiskID, vdiskID, templateCluster, cluster)

	// get from storage
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
	data, err := ardb.Int64ToBytesMapping(
		clusterA.Do(ardb.Command(command.HashGetAll, lba.StorageKey(vdiskIDA))))
	if err != nil {
		panicOnError(err)
	}

	cmds := []ardb.StorageAction{ardb.Command(command.Delete, lba.StorageKey(vdiskIDB))}
	for index, hash := range data {
		cmds = append(cmds,
			ardb.Command(command.HashSet, lba.StorageKey(vdiskIDB), index, hash))
	}

	_, err = clusterB.Do(ardb.Commands(cmds...))
	if err != nil {
		panicOnError(err)
	}
}
