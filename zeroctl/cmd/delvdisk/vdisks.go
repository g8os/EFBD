package delvdisk

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdisksCfg struct {
	ConfigPath string
	Force      bool
}

// VdisksCmd represents the vdisks delete subcommand
var VdisksCmd = &cobra.Command{
	Use:   "vdisks [vdiskid...]",
	Short: "Delete one, multiple or all vdisks",
	RunE:  deleteVdisks,
}

func deleteVdisks(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.Infof("loading config %s...", vdisksCfg.ConfigPath)

	cfg, err := config.ReadConfig(vdisksCfg.ConfigPath, config.NBDServer)
	if err != nil {
		return err
	}

	vdisks, err := getVdisks(cfg, args)
	if err != nil {
		return err
	}

	// store all requests in a map, sorted per storage server,
	// such that we only require 1 delete pipeline per storage server.
	delControllerMap := make(map[config.StorageServerConfig][]delController)

	var storage config.StorageServerConfig
	var storageType config.StorageType

	log.Info("sorting all target vdisks by storage type and connection")
	for vdiskID, vdisk := range vdisks {
		// storageCluster is guaranteed to exist by the config module
		cluster := cfg.StorageClusters[vdisk.StorageCluster]

		switch storageType = vdisk.StorageType(); storageType {
		// add deduped delete controllers
		case config.StorageDeduped:
			storage = *cluster.MetadataStorage
			delControllerMap[storage] = append(
				delControllerMap[storage], newDedupDelController(vdiskID))

		// add non deduped delete controllers
		case config.StorageNonDeduped:
			for _, storage = range cluster.DataStorage {
				delControllerMap[storage] = append(
					delControllerMap[storage], newNonDedupDelController(vdiskID))
			}

		// add semi deduped delete controllers
		case config.StorageSemiDeduped:
			storage = *cluster.MetadataStorage
			delControllerMap[storage] = append(
				delControllerMap[storage],
				newDedupDelController(vdiskID), newSemiDedupDelController(vdiskID))

			for _, storage = range cluster.DataStorage {
				delControllerMap[storage] = append(
					delControllerMap[storage], newNonDedupDelController(vdiskID))
			}

		default: // shouldn't happen
			return fmt.Errorf("invariant: vdisk %s has unknown storage type %d",
				vdiskID, storageType)
		}
	}

	for cfg, controllers := range delControllerMap {
		err = deleleFromStorageServer(vdisksCfg.Force, cfg, controllers...)
		if err != nil {
			if !vdisksCfg.Force {
				return err
			}

			log.Error(err)
		}
	}

	log.Info("done")
	return nil
}

func getVdisks(cfg *config.Config, args []string) (map[string]config.VdiskConfig, error) {
	if len(args) == 0 {
		return cfg.Vdisks, nil
	}

	// create a vdisk map, so we only have each id once
	vdiskids := make(map[string]struct{}, len(args))
	for _, vdiskid := range args {
		vdiskids[vdiskid] = struct{}{}
	}

	log.Info("retreiving given vdisks from config file...")

	// collect all vdisk configurations (once)
	vdisks := make(map[string]config.VdiskConfig)
	for candidateID := range vdiskids {
		for vdiskID := range cfg.Vdisks {
			if vdiskID == candidateID {
				vdisks[vdiskID] = cfg.Vdisks[vdiskID]
				delete(cfg.Vdisks, vdiskID)
				delete(vdiskids, vdiskID)
				break
			}
		}

		if len(cfg.Vdisks) == 0 {
			break
		}
	}

	if len(vdisks) == 0 {
		return nil, errors.New("no vdisks could be found for the given vdiskids")
	}

	if !vdisksCfg.Force && len(vdiskids) > 0 {
		// abort non-forced command,
		// in case least one given vdisk could not be found

		message := "following vdisk(s) could not be found in the config file:"
		for vdiskID := range vdiskids {
			message += " " + vdiskID + ","
		}
		return nil, errors.New(message[:len(message)-1])
	}

	// log an error for each invalid vdisk id
	for vdiskID := range vdiskids {
		log.Errorf("vdisk %s could not be found and will thus not be deleted", vdiskID)
	}

	return vdisks, nil
}

// delete the semideduped vdisks
func deleleFromStorageServer(force bool, cfg config.StorageServerConfig, controllers ...delController) error {
	if len(controllers) == 0 {
		return nil // no controllers defined, returning early
	}

	// open redis connection
	log.Infof("dialing redis TCP connection at: %s (%d)", cfg.Address, cfg.Database)
	conn, err := redis.Dial("tcp", cfg.Address, redis.DialDatabase(cfg.Database))
	if err != nil {
		return err
	}
	defer conn.Close()

	// add each delete request to the pipeline
	var batchedControllers []delController
	for _, controller := range controllers {
		err = controller.BatchRequest(conn)
		if err != nil {
			if !force {
				return err
			}

			log.Error(err)
			continue
		}
		batchedControllers = append(batchedControllers, controller)
	}

	// flush all delete requests
	err = conn.Flush()
	if err != nil {
		return fmt.Errorf("could not flush request for conn %s (%d): %v",
			cfg.Address, cfg.Database, err)
	}

	// check if all delete operations were successfull
	for _, controller := range batchedControllers {
		err = controller.CheckRequest(conn)
		// content was deleted, no error, Yay!
		if err == nil {
			continue
		}

		// no critical error, content simply didn't exist, so couldn't be deleted
		if err, ok := err.(noDeleteError); ok {
			log.Infof("%v: did not exist at %s (%d)", err, cfg.Address, cfg.Database)
			continue
		}

		// critical error while deleting content

		if !force {
			return err
		}

		log.Error(err)
	}

	return nil
}

// delController is a generic interface, which allows us to batch & check
// a deletion request, pipelined in a series of deletion processes
// for a single storage server.
type delController interface {
	// BatchRequest adds the request to the conn's pipeline
	BatchRequest(conn redis.Conn) error
	// CheckRequest checks if the earlier batch request has been processed fine
	CheckRequest(conn redis.Conn) error
}

// noDeleteError can be returned by delController.CheckRequest,
// in case there was no real error, but nothing got deleted either,
// probably because it didn't exist
type noDeleteError error

func init() {
	VdisksCmd.Long = VdisksCmd.Short + `

When no vdiskids are specified,
all vdisks listed in the config file will be deleted.

WARNING: until issue #88 has been resolved,
  only the metadata of deduped vdisks can be deleted by this command.
  Nondeduped vdisks have no metadata, and thus are not affected by this issue.
`

	VdisksCmd.Flags().StringVar(
		&vdisksCfg.ConfigPath, "config", "config.yml",
		"zeroctl config file")
	VdisksCmd.Flags().BoolVarP(
		&vdisksCfg.Force, "force", "f", false,
		"when enabled non-fatal errors are logged instead of aborting the command")
}
