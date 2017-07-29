package delvdisk

import (
	"errors"

	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	cmdconfig "github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCmdCfg struct {
	RawConfigResource string
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

	configInfo, err := zerodisk.ParseConfigInfo(vdiskCmdCfg.RawConfigResource)
	if err != nil {
		return err
	}

	if len(args) == 0 {
		return errors.New("no vdisk identifiers given")
	}

	// get and sort vdisks per server cfg
	data, metadata, err := getAndSortVdisks(*configInfo, args)
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

func getAndSortVdisks(configInfo zerodisk.ConfigInfo, vdiskIDs []string) (data vdisksPerServerMap, metadata vdisksPerServerMap, err error) {
	if len(vdiskIDs) == 0 {
		err = errors.New("no vdisk identifiers given")
	}

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

	var baseConfig *config.BaseConfig
	var nbdConfig *config.NBDConfig

	// add only the selected vdisk(s)
	for _, vdiskID := range vdiskIDs {
		baseConfig, nbdConfig, err = zerodisk.ReadNBDConfig(vdiskID, configInfo)
		if err != nil {
			log.Errorf("no NBD config could be retrieved for %s: %v", vdiskID, err)
			continue
		}

		addVdiskCluster(nbdConfig.StorageCluster, vdiskID, baseConfig.Type)
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
		&vdiskCmdCfg.RawConfigResource, "config", "config.yml",
		"config resource: etcd (dialstring(s)) or file (path)")
}
