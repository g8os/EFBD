package copyvdisk

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCfg struct {
	ConfigPath string
}

// VdiskCmd represents the vdisk copy subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisk source_vdiskid target_vdiskid [target_cluster]",
	Short: "Copy a vdisk configured in the config file",
	RunE:  copyVdisk,
}

func copyVdisk(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.Info("parsing positional arguments...")

	// validate pos arg length
	argn := len(args)
	if argn < 2 {
		return errors.New("not enough arguments")
	} else if argn > 3 {
		return errors.New("too many arguments")
	}

	// store pos arguments in named variables
	sourceVdiskID, targetVdiskID := args[0], args[1]
	var targetStorageCluster string
	if argn == 3 {
		targetStorageCluster = args[2]
	}

	log.Infof("loading config %s...", vdiskCfg.ConfigPath)

	cfg, err := config.ReadConfig(vdiskCfg.ConfigPath, config.NBDServer)
	if err != nil {
		return err
	}

	// get source vdisk, and ensure that targetCluster has a valid value
	sourceVdisk, ok := cfg.Vdisks[sourceVdiskID]
	if !ok {
		return fmt.Errorf("vdisk %s could not be found in config %s",
			sourceVdiskID, vdiskCfg.ConfigPath)
	}
	if targetStorageCluster == "" {
		if sourceVdiskID == targetVdiskID {
			return errors.New("cannot copy a vdisk to itself on the same storage cluster")
		}

		targetStorageCluster = sourceVdisk.StorageCluster
	}

	// copy the vdisk
	switch stype := sourceVdisk.StorageType(); stype {
	case config.StorageDeduped:
		return copyDedupedVdisk(
			sourceVdiskID, targetVdiskID,
			cfg.StorageClusters[sourceVdisk.StorageCluster],
			cfg.StorageClusters[targetStorageCluster])
	case config.StorageNonDeduped:
		return copyNonDedupedVdisk(
			sourceVdiskID, targetVdiskID,
			cfg.StorageClusters[sourceVdisk.StorageCluster],
			cfg.StorageClusters[targetStorageCluster])
	case config.StorageSemiDeduped:
		return copySemiDedupedVdisk(
			sourceVdiskID, targetVdiskID,
			cfg.StorageClusters[sourceVdisk.StorageCluster],
			cfg.StorageClusters[targetStorageCluster])
	default:
		return fmt.Errorf("vdisk %s has an unknown storage type %d",
			sourceVdiskID, stype)
	}
}

// NOTE: copies metadata only!
func copyDedupedVdisk(sourceID, targetID string, sourceCluster, targetCluster config.StorageClusterConfig) error {
	if sourceCluster.MetadataStorage == nil {
		return errors.New("no metaDataServer given for source")
	}
	if targetCluster.MetadataStorage == nil {
		return errors.New("no metaDataServer given for target")
	}

	// within same storage server
	if *sourceCluster.MetadataStorage == *targetCluster.MetadataStorage {
		conn, err := getConnection(*sourceCluster.MetadataStorage)
		if err != nil {
			return fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
		}
		defer conn.Close()

		return copyDedupedSameConnection(sourceID, targetID, conn)
	}

	// between different storage servers
	connA, connB, err := getConnections(
		*sourceCluster.MetadataStorage, *targetCluster.MetadataStorage)
	if err != nil {
		return fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer func() {
		connA.Close()
		connB.Close()
	}()

	return copyDedupedDifferentConnections(sourceID, targetID, connA, connB)
}

// NOTE: copies data only (as there is no metadata for nondeduped vdisks)
func copyNonDedupedVdisk(sourceID, targetID string, sourceCluster, targetCluster config.StorageClusterConfig) error {
	sourceDataServerCount := len(sourceCluster.DataStorage)
	targetDataServerCount := len(targetCluster.DataStorage)

	if targetDataServerCount != sourceDataServerCount {
		return errors.New("target data server count has to equal the source data server count")
	}

	var sourceCfg, targetCfg config.StorageServerConfig
	// WARNING: [TODO]
	// Currently the result will be WRONG in case targetDataServerCount != sourceDataServerCount,
	// as the storage data spread will not be the same,
	// to what the nbdserver read calls will expect.
	// See open issue for more information:
	// https://github.com/zero-os/0-Disk/issues/206
	for i := 0; i < sourceDataServerCount; i++ {
		sourceCfg = sourceCluster.DataStorage[i]
		targetCfg = targetCluster.DataStorage[i]

		// within same storage server
		if sourceCfg == targetCfg {
			conn, err := getConnection(sourceCfg)
			if err != nil {
				return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
			}
			defer conn.Close()

			return copyNonDedupedSameConnection(sourceID, targetID, conn)
		}

		// between different storage servers
		connA, connB, err := getConnections(sourceCfg, targetCfg)
		if err != nil {
			return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
		}
		defer func() {
			connA.Close()
			connB.Close()
		}()

		err = copyNonDedupedDifferentConnections(sourceID, targetID, connA, connB)
		if err != nil {
			return err
		}
	}

	return nil
}

func copySemiDedupedVdisk(sourceID, targetID string, sourceCluster, targetCluster config.StorageClusterConfig) error {
	if sourceCluster.MetadataStorage == nil {
		return errors.New("no metaDataServer given for source")
	}
	if targetCluster.MetadataStorage == nil {
		return errors.New("no metaDataServer given for target")
	}

	var hasBitMask bool
	var err error

	// within same meta storage server
	if *sourceCluster.MetadataStorage == *targetCluster.MetadataStorage {
		// copy metadata from the same storage server
		hasBitMask, err = func() (hasBitMask bool, err error) {
			conn, err := getConnection(*sourceCluster.MetadataStorage)
			if err != nil {
				err = fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
				return
			}
			defer conn.Close()

			// copy metadata of deduped metadata
			err = copyDedupedSameConnection(sourceID, targetID, conn)
			if err != nil {
				err = fmt.Errorf("couldn't copy deduped metadata: %s", err.Error())
				return
			}

			// copy bitmask
			hasBitMask, err = copySemiDedupedSameConnection(sourceID, targetID, conn)
			return
		}()
	} else {
		// copy metadata from different storage servers
		hasBitMask, err = func() (hasBitMask bool, err error) {
			connA, connB, err := getConnections(*sourceCluster.MetadataStorage, *targetCluster.MetadataStorage)
			if err != nil {
				err = fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
				return
			}
			defer func() {
				connA.Close()
				connB.Close()
			}()

			// copy metadata of deduped metadata
			err = copyDedupedDifferentConnections(sourceID, targetID, connA, connB)
			if err != nil {
				err = fmt.Errorf("couldn't copy deduped metadata: %s", err.Error())
				return
			}

			// copy bitmask
			hasBitMask, err = copySemiDedupedDifferentConnections(sourceID, targetID, connA, connB)
			return
		}()
	}

	if err != nil {
		return fmt.Errorf("couldn't copy bitmask: %v", err)
	}
	if !hasBitMask {
		// no bitmask == no nondeduped content,
		// which means this is an untouched semideduped storage
		return nil // nothing to do, early return
	}

	// dispatch the rest of the work to the copyNonDedupedVdisk func,
	// to copy all the user (nondeduped) data
	err = copyNonDedupedVdisk(sourceID, targetID, sourceCluster, targetCluster)
	if err != nil {
		return fmt.Errorf("couldn't copy nondeduped content: %v", err)
	}

	// copy went ALL-OK!
	return nil
}

func getConnection(cfg config.StorageServerConfig) (redis.Conn, error) {
	return redis.Dial("tcp", cfg.Address, redis.DialDatabase(cfg.Database))
}

func getConnections(cfgA, cfgB config.StorageServerConfig) (connA redis.Conn, connB redis.Conn, err error) {
	connA, err = getConnection(cfgA)
	if err != nil {
		return
	}

	connB, err = getConnection(cfgB)
	if err != nil {
		connA.Close()
		connA = nil
		return
	}

	return
}

func init() {
	VdiskCmd.Long = VdiskCmd.Short + `

If no target storage cluster is given,
the storage cluster configured for the source vdisk
will also be used for the target vdisk.

If an error occured, the target vdisk should be considered as non-existent,
even though data which is already copied is not rolled back.

NOTE: by design,
  only the metadata of a deduped vdisk is copied,
  the data will be copied the first time the vdisk spins up,
  on the condition that the templateStorageCluster has been configured.

WARNING: when copying nondeduped vdisks,
  it is currently not supported that the target vdisk's data cluster
  has more or less storage servers, then the source vdisk's data cluster.
  See issue #206 for more information.
`

	VdiskCmd.Flags().StringVar(
		&vdiskCfg.ConfigPath, "config", "config.yml",
		"zeroctl config file")
}
