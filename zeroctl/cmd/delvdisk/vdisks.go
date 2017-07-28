package delvdisk

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
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
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	log.Infof("loading config %s...", vdisksCfg.ConfigPath)

	cfg, err := config.ReadConfig(vdisksCfg.ConfigPath, config.NBDServer)
	if err != nil {
		return err
	}

	data, metadata, err := getAndSortVdisks(cfg, args)
	if err != nil {
		return err
	}

	var errs []error

	for serverCfg, vdisks := range metadata {
		err = storage.DeleteMetadata(serverCfg, vdisks)
		if err != nil {
			log.Error(err)
			errs = append(errs, err)
		}
	}

	for serverCfg, vdisks := range data {
		err = storage.DeleteData(serverCfg, vdisks)
		if err != nil {
			log.Error(err)
			errs = append(errs, err)
		}
	}

	if errs != nil {
		var str string
		for _, err := range errs {
			str += err.Error() + ", "
		}

		return errors.New(str[:len(str)-2])
	}

	return nil
}

type vdisksPerServerMap map[config.StorageServerConfig]map[string]config.VdiskType

func (m vdisksPerServerMap) AddVdisk(cfg config.StorageServerConfig, vdiskID string, vdiskType config.VdiskType) {
	serverVdisks, ok := m[cfg]
	if !ok {
		serverVdisks = make(map[string]config.VdiskType)
		m[cfg] = serverVdisks
	}

	serverVdisks[vdiskID] = vdiskType
}

func getAndSortVdisks(cfg *config.Config, args []string) (data vdisksPerServerMap, metadata vdisksPerServerMap, err error) {
	data = make(vdisksPerServerMap)
	metadata = make(vdisksPerServerMap)

	addVdiskCluster := func(cfg config.StorageClusterConfig, vdiskID string, vdiskType config.VdiskType) {
		if cfg.MetadataStorage != nil {
			metadata.AddVdisk(*cfg.MetadataStorage, vdiskID, vdiskType)
		}

		for _, serverCfg := range cfg.DataStorage {
			data.AddVdisk(serverCfg, vdiskID, vdiskType)
		}
	}

	var clusterCfg config.StorageClusterConfig

	if len(args) != 0 {
		var ok bool
		var vdiskCfg config.VdiskConfig

		// add only the selected vdisk(s)
		for _, arg := range args {
			vdiskCfg, ok = cfg.Vdisks[arg]
			if !ok {
				log.Errorf("no config found for vdisk %s", arg)
				continue
			}

			clusterCfg = cfg.StorageClusters[vdiskCfg.StorageCluster]
			addVdiskCluster(clusterCfg, arg, vdiskCfg.Type)
		}
	} else {
		// add all available vdisk(s)
		for vdiskID, vdiskCfg := range cfg.Vdisks {
			clusterCfg = cfg.StorageClusters[vdiskCfg.StorageCluster]
			addVdiskCluster(clusterCfg, vdiskID, vdiskCfg.Type)
		}
	}

	if len(data) == 0 && len(metadata) == 0 {
		return nil, nil, errors.New("no given vdisk could be found to be deleted")
	}

	return data, metadata, nil
}

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
