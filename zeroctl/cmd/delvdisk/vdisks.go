package delvdisk

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/nbd/nbdserver/tlog"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCmdCfg struct {
	SourceConfig config.SourceConfig
}

// VdisksCmd represents the vdisks delete subcommand
var VdisksCmd = &cobra.Command{
	Use:   "vdisks vdiskid...",
	Short: "Delete one, multiple or all vdisks",
	RunE:  deleteVdisks,
}

func deleteVdisks(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if cmdconfig.Verbose {
		logLevel = log.DebugLevel
	}
	log.SetLevel(logLevel)

	// create config source
	configSource, err := config.NewSource(vdiskCmdCfg.SourceConfig)
	if err != nil {
		return err
	}
	defer configSource.Close()

	if len(args) == 0 {
		return errors.New("no vdisk identifiers given")
	}

	// get and sort vdisks per server cfg
	data, metadata, err := getAndSortVdisks(args, configSource)
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

		// delete tlog-specific metadata of tlog-enabled vdisks
		err = deleteTlogMetadata(serverCfg, vdisks)
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

func deleteTlogMetadata(serverCfg config.StorageServerConfig, vdiskMap map[string]config.VdiskType) error {
	var vdisks []string
	for vdiskID, vdiskType := range vdiskMap {
		if vdiskType.TlogSupport() {
			vdisks = append(vdisks, vdiskID)
		}
	}

	// TODO: ensure that vdisk also have an active tlog configuration,
	//       as this is still optional even though it might support it type-wise.
	// TODO: also delete actual tlog meta(data) from 0-Stor cluster for the supported vdisks
	//       https://github.com/zero-os/0-Disk/issues/147

	return tlog.DeleteMetadata(serverCfg, vdisks...)
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

func getAndSortVdisks(vdiskIDs []string, configSource config.Source) (data vdisksPerServerMap, metadata vdisksPerServerMap, err error) {
	if len(vdiskIDs) == 0 {
		err = errors.New("no vdisk identifiers given")
	}

	data = make(vdisksPerServerMap)
	metadata = make(vdisksPerServerMap)

	addVdiskCluster := func(cfg config.StorageClusterConfig, vdiskID string, vdiskType config.VdiskType) {
		if vdiskType.TlogSupport() || vdiskType.StorageType() == config.StorageSemiDeduped {
			storageServerCfg, err := cfg.FirstAvailableServer()
			// TODO: fix this properly
			// see https://github.com/zero-os/0-Disk/issues/481
			if err != nil {
				metadata.AddVdisk(*storageServerCfg, vdiskID, vdiskType)
			}
		}

		for _, serverCfg := range cfg.DataStorage {
			data.AddVdisk(serverCfg, vdiskID, vdiskType)
		}
	}

	var vdiskConfig *config.VdiskStaticConfig
	var nbdConfig *config.NBDStorageConfig

	// add only the selected vdisk(s)
	for _, vdiskID := range vdiskIDs {
		vdiskConfig, err = config.ReadVdiskStaticConfig(configSource, vdiskID)
		if err != nil {
			log.Errorf("no static vdisk config could be retrieved for %s: %v", vdiskID, err)
			continue
		}
		nbdConfig, err = config.ReadNBDStorageConfig(configSource, vdiskID)
		if err != nil {
			log.Errorf("no nbd config could be retrieved for %s: %v", vdiskID, err)
			continue
		}

		addVdiskCluster(nbdConfig.StorageCluster, vdiskID, vdiskConfig.Type)
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

	VdisksCmd.Flags().Var(
		&vdiskCmdCfg.SourceConfig, "config",
		"config resource: dialstrings (etcd cluster) or path (yaml file)")
}
