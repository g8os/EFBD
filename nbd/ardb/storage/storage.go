package storage

import (
	"context"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// BlockStorage defines an interface for all a block storage.
// It can be used to set, get and delete blocks.
//
// It is used by the `nbdserver.Backend` to implement the NBD Backend,
// as well as other modules, who need to manipulate the block storage for whatever reason.
type BlockStorage interface {
	SetBlock(blockIndex int64, content []byte) (err error)
	GetBlock(blockIndex int64) (content []byte, err error)
	DeleteBlock(blockIndex int64) (err error)

	Flush() (err error)
	Close() (err error)
}

// BlockStorageConfig is used when creating a block storage using the
// NewBlockStorage helper constructor.
type BlockStorageConfig struct {
	// required: ID of the vdisk
	VdiskID string
	// optional: used for nondeduped storage
	TemplateVdiskID string

	// required: type of vdisk
	VdiskType config.VdiskType

	// required: block size in bytes
	BlockSize int64

	// optional: used by (semi)deduped storage
	LBACacheLimit int64
}

// Validate this BlockStorageConfig.
func (cfg *BlockStorageConfig) Validate() error {
	if cfg == nil {
		return nil
	}

	if err := cfg.VdiskType.Validate(); err != nil {
		return err
	}

	if !config.ValidateBlockSize(cfg.BlockSize) {
		return errors.New("invalid block size size")
	}

	return nil
}

// BlockStorageFromConfig creates a block storage
// from the config retrieved from the given config source.
// It is the simplest way to create a BlockStorage,
// but it also has the disadvantage that
// it does not support SelfHealing or HotReloading of the used configuration.
func BlockStorageFromConfig(vdiskID string, cs config.Source, dialer ardb.ConnectionDialer) (BlockStorage, error) {
	// get configs from source
	vdiskConfig, err := config.ReadVdiskStaticConfig(cs, vdiskID)
	if err != nil {
		return nil, err
	}
	nbdStorageConfig, err := config.ReadNBDStorageConfig(cs, vdiskID)
	if err != nil {
		return nil, err
	}

	err = vdiskConfig.Validate()
	if err != nil {
		return nil, err
	}
	err = nbdStorageConfig.Validate()
	if err != nil {
		return nil, err
	}

	// create primary cluster
	cluster, err := ardb.NewCluster(nbdStorageConfig.StorageCluster, dialer)
	if err != nil {
		return nil, err
	}

	// create template cluster if needed
	var templateCluster ardb.StorageCluster
	if vdiskConfig.Type.TemplateSupport() && nbdStorageConfig.TemplateStorageCluster != nil {
		templateCluster, err = ardb.NewCluster(*nbdStorageConfig.TemplateStorageCluster, dialer)
		if err != nil {
			return nil, err
		}
	}

	// create block storage config
	cfg := BlockStorageConfig{
		VdiskID:         vdiskID,
		TemplateVdiskID: vdiskConfig.TemplateVdiskID,
		VdiskType:       vdiskConfig.Type,
		BlockSize:       int64(vdiskConfig.BlockSize),
		LBACacheLimit:   ardb.DefaultLBACacheLimit,
	}

	// try to create actual block storage
	return NewBlockStorage(cfg, cluster, templateCluster)
}

// NewBlockStorage returns the correct block storage based on the given VdiskConfig.
func NewBlockStorage(cfg BlockStorageConfig, cluster, templateCluster ardb.StorageCluster) (storage BlockStorage, err error) {
	err = cfg.Validate()
	if err != nil {
		return
	}

	vdiskType := cfg.VdiskType

	// templateCluster gets disabled,
	// if vdisk type has no template support.
	if !vdiskType.TemplateSupport() {
		templateCluster = nil
	}

	switch storageType := vdiskType.StorageType(); storageType {
	case config.StorageDeduped:
		return Deduped(
			cfg.VdiskID,
			cfg.BlockSize,
			cfg.LBACacheLimit,
			cluster,
			templateCluster)

	case config.StorageNonDeduped:
		return NonDeduped(
			cfg.VdiskID,
			cfg.TemplateVdiskID,
			cfg.BlockSize,
			cluster,
			templateCluster)

	case config.StorageSemiDeduped:
		return SemiDeduped(
			cfg.VdiskID,
			cfg.BlockSize,
			cfg.LBACacheLimit,
			cluster,
			templateCluster)

	default:
		return nil, errors.Newf(
			"no block storage available for %s's storage type %s",
			cfg.VdiskID, storageType)
	}
}

// VdiskExists returns true if the vdisk in question exists in the given ARDB storage cluster.
// An error is returned in case this couldn't be verified for whatever reason.
// Also return vdiskType and ardb cluster from config
func VdiskExists(vdiskID string, source config.Source) (bool, config.VdiskType, *ardb.Cluster, error) {
	// gather configs
	staticConfig, err := config.ReadVdiskStaticConfig(source, vdiskID)
	if err != nil {
		return false, staticConfig.Type, nil, errors.Wrapf(err,
			"cannot read static vdisk config for vdisk %s", vdiskID)
	}
	nbdConfig, err := config.ReadVdiskNBDConfig(source, vdiskID)
	if err != nil {
		return false, staticConfig.Type, nil, errors.Wrapf(err,
			"cannot read nbd storage config for vdisk %s", vdiskID)
	}
	clusterConfig, err := config.ReadStorageClusterConfig(source, nbdConfig.StorageClusterID)
	if err != nil {
		return false, staticConfig.Type, nil, errors.Wrapf(err,
			"cannot read storage cluster config for cluster %s",
			nbdConfig.StorageClusterID)
	}

	// create (primary) storage cluster
	cluster, err := ardb.NewCluster(*clusterConfig, nil) // not pooled
	if err != nil {
		return false, staticConfig.Type, nil, errors.Wrapf(err,
			"cannot create storage cluster model for cluster %s",
			nbdConfig.StorageClusterID)
	}

	exists, err := VdiskExistsInCluster(vdiskID, staticConfig.Type, cluster)

	return exists, staticConfig.Type, cluster, err
}

// VdiskExistsInCluster returns true if the vdisk in question exists in the given ARDB storage cluster.
// An error is returned in case this couldn't be verified for whatever reason.
func VdiskExistsInCluster(vdiskID string, t config.VdiskType, cluster ardb.StorageCluster) (bool, error) {
	switch st := t.StorageType(); st {
	case config.StorageDeduped:
		return dedupedVdiskExists(vdiskID, cluster)

	case config.StorageNonDeduped:
		return nonDedupedVdiskExists(vdiskID, cluster)

	case config.StorageSemiDeduped:
		return semiDedupedVdiskExists(vdiskID, cluster)

	default:
		return false, errors.Newf("%v is not a supported storage type", st)
	}
}

// CopyVdiskConfig is the config for a vdisk
// used when calling the CopyVdisk primitive.
type CopyVdiskConfig struct {
	VdiskID   string
	Type      config.VdiskType
	BlockSize int64
}

// CopyVdisk allows you to copy a vdisk from a source to a target vdisk.
// The source and target vdisks have to have the same storage type and block size.
// They can be stored on the same or different clusters.
func CopyVdisk(source, target CopyVdiskConfig, sourceCluster, targetCluster ardb.StorageCluster) error {
	sourceStorageType := source.Type.StorageType()
	targetStorageType := target.Type.StorageType()
	if sourceStorageType != targetStorageType {
		return errors.Newf(
			"source vdisk %s and target vdisk %s have different storageTypes (%s != %s)",
			source.VdiskID, target.VdiskID, sourceStorageType, targetStorageType)
	}

	var err error
	switch sourceStorageType {
	case config.StorageDeduped:
		err = copyDedupedMetadata(
			source.VdiskID, target.VdiskID, source.BlockSize, target.BlockSize,
			sourceCluster, targetCluster)

	case config.StorageNonDeduped:
		err = copyNonDedupedData(
			source.VdiskID, target.VdiskID, source.BlockSize, target.BlockSize,
			sourceCluster, targetCluster)

	case config.StorageSemiDeduped:
		err = copySemiDeduped(
			source.VdiskID, target.VdiskID, source.BlockSize, target.BlockSize,
			sourceCluster, targetCluster)

	default:
		err = errors.Newf(
			"%v is not a supported storage type", sourceStorageType)
	}

	if err != nil || !source.Type.TlogSupport() || !target.Type.TlogSupport() {
		return err
	}

	return copyTlogMetadata(source.VdiskID, target.VdiskID, sourceCluster, targetCluster)
}

// DeleteVdisk returns true if the vdisk in question was deleted from the given ARDB storage cluster.
// An error is returned in case this couldn't be deleted (completely) for whatever reason.
func DeleteVdisk(vdiskID string, configSource config.Source) (bool, error) {
	staticConfig, err := config.ReadVdiskStaticConfig(configSource, vdiskID)
	if err != nil {
		return false, err
	}
	nbdConfig, err := config.ReadVdiskNBDConfig(configSource, vdiskID)
	if err != nil {
		return false, err
	}
	clusterConfig, err := config.ReadStorageClusterConfig(configSource, nbdConfig.StorageClusterID)
	if err != nil {
		return false, err
	}

	cluster, err := ardb.NewCluster(*clusterConfig, nil)
	if err != nil {
		return false, err
	}

	return DeleteVdiskInCluster(vdiskID, staticConfig.Type, cluster)
}

// DeleteVdiskInCluster returns true if the vdisk in question was deleted from the given ARDB storage cluster.
// An error is returned in case this couldn't be deleted (completely) for whatever reason.
func DeleteVdiskInCluster(vdiskID string, t config.VdiskType, cluster ardb.StorageCluster) (bool, error) {
	var err error
	var deletedTlogMetadata bool
	if t.TlogSupport() {
		command := ardb.Command(command.Delete, tlogMetadataKey(vdiskID))
		deletedTlogMetadata, err = ardb.Bool(cluster.Do(command))
		if err != nil {
			return false, err
		}
		if deletedTlogMetadata {
			log.Infof("deleted tlog metadata stored for vdisk %s on first available server", vdiskID)
		}
	}

	var deletedStorage bool
	switch st := t.StorageType(); st {
	case config.StorageDeduped:
		deletedStorage, err = deleteDedupedData(vdiskID, cluster)
	case config.StorageNonDeduped:
		deletedStorage, err = deleteNonDedupedData(vdiskID, cluster)
	case config.StorageSemiDeduped:
		deletedStorage, err = deleteSemiDedupedData(vdiskID, cluster)
	default:
		err = errors.Newf("%v is not a supported storage type", st)
	}

	return deletedTlogMetadata || deletedStorage, err
}

// ListVdisks scans a given storage cluster
// for available vdisks, and returns their ids.
// Optionally a predicate can be given to
// filter specific vdisks based on their identifiers.
// NOTE: this function is very slow,
//       and puts a lot of pressure on the ARDB cluster.
func ListVdisks(cluster ardb.StorageCluster, pred func(vdiskID string) bool) ([]string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverCh, err := cluster.ServerIterator(ctx)
	if err != nil {
		return nil, err
	}

	type serverResult struct {
		ids []string
		err error
	}
	resultCh := make(chan serverResult)

	var action listVdisksAction
	if pred == nil {
		action.filter = filterListedVdiskID
	} else {
		action.filter = func(str string) (string, bool) {
			str, ok := filterListedVdiskID(str)
			if !ok {
				return "", false
			}
			return str, pred(str)
		}
	}

	var serverCount int
	var reply interface{}
	for server := range serverCh {
		server := server
		go func() {
			var result serverResult
			log.Infof("listing all vdisks stored on %v", server.Config())
			reply, result.err = server.Do(action)
			if result.err == nil && reply != nil {
				// [NOTE] this line of code relies on the fact that our
				// custom `listVdisksAction` type returns a `[]string` value as a reply,
				// as soon as that logic changes, this line will start causing trouble.
				result.ids = reply.([]string)
			}
			select {
			case resultCh <- result:
			case <-ctx.Done():
			}
		}()
		serverCount++
	}

	// collect the ids from all servers within the given cluster
	var ids []string
	var result serverResult
	for i := 0; i < serverCount; i++ {
		result = <-resultCh
		if result.err != nil {
			// return early, an error has occured!
			return nil, result.err
		}
		ids = append(ids, result.ids...)
	}

	if len(ids) <= 1 {
		return ids, nil // nothing to do
	}

	// sort and dedupe
	sort.Strings(ids)
	ids = dedupStrings(ids)

	return ids, nil
}

type listVdisksAction struct {
	filter func(string) (string, bool)
}

// Do implements StorageAction.Do
func (action listVdisksAction) Do(conn ardb.Conn) (reply interface{}, err error) {
	const (
		startCursor = "0"
		itemCount   = "5000"
	)

	var output, vdisks []string
	var slice interface{}

	// initial cursor and action
	cursor := startCursor
	scan := ardb.Command(command.Scan, cursor, "COUNT", itemCount)

	// go through all available keys
	for {
		// get new cursor and raw data
		cursor, slice, err = ardb.CursorAndValues(scan.Do(conn))
		// convert the raw data to a string slice we can use
		output, err = ardb.Strings(slice, err)
		// return early in case of error
		if err != nil {
			return nil, err
		}

		// filter output
		filterPos := 0
		var ok bool
		var vdiskID string
		for i := range output {
			vdiskID, ok = action.filter(output[i])
			if ok {
				output[filterPos] = vdiskID
				filterPos++
			}
		}
		output = output[:filterPos]
		vdisks = append(vdisks, output...)
		log.Debugf("%d/%s identifiers in iteration which match the given filters",
			len(output), itemCount)

		// stop in case we iterated through all possible values
		if cursor == startCursor || cursor == "" {
			break
		}

		scan = ardb.Command(command.Scan, cursor, "COUNT", itemCount)
	}

	return vdisks, nil
}

// Send implements StorageAction.Send
func (action listVdisksAction) Send(conn ardb.Conn) error {
	return ErrMethodNotSupported
}

// KeysModified implements StorageAction.KeysModified
func (action listVdisksAction) KeysModified() ([]string, bool) {
	return nil, false
}

// ListBlockIndices returns all indices stored for the given vdisk from a config source.
func ListBlockIndices(vdiskID string, source config.Source) ([]int64, error) {
	staticConfig, err := config.ReadVdiskStaticConfig(source, vdiskID)
	if err != nil {
		return nil, err
	}

	nbdConfig, err := config.ReadNBDStorageConfig(source, vdiskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to ReadNBDStorageConfig")
	}

	// create (primary) storage cluster
	// TODO: support optional slave cluster here
	// see: https://github.com/zero-os/0-Disk/issues/445
	cluster, err := ardb.NewCluster(nbdConfig.StorageCluster, nil) // not pooled
	if err != nil {
		return nil, errors.Wrapf(err,
			"cannot create storage cluster model for primary cluster of vdisk %s",
			vdiskID)
	}

	return ListBlockIndicesInCluster(vdiskID, staticConfig.Type, cluster)
}

// ListBlockIndicesInCluster returns all indices stored for the given vdisk from cluster configs.
// This function returns either an error OR indices.
func ListBlockIndicesInCluster(id string, t config.VdiskType, cluster ardb.StorageCluster) ([]int64, error) {
	switch st := t.StorageType(); st {
	case config.StorageDeduped:
		return listDedupedBlockIndices(id, cluster)

	case config.StorageNonDeduped:
		return listNonDedupedBlockIndices(id, cluster)

	case config.StorageSemiDeduped:
		return listSemiDedupedBlockIndices(id, cluster)

	default:
		return nil, errors.Newf("%v is not a supported storage type", st)
	}
}

// filterListedVdiskID only accepts keys with a known prefix,
// if no known prefix is found an empty string is returned,
// otherwise the prefix is removed and the vdiskID is returned.
func filterListedVdiskID(key string) (string, bool) {
	parts := listStorageKeyPrefixRex.FindStringSubmatch(key)
	if len(parts) == 3 {
		return parts[2], true
	}

	return "", false
}

var listStorageKeyPrefixRex = regexp.MustCompile("^(" +
	strings.Join(listStorageKeyPrefixes, "|") +
	")(.+)$")

var listStorageKeyPrefixes = []string{
	lbaStorageKeyPrefix,
	nonDedupedStorageKeyPrefix,
}

// sortInt64s sorts a slice of int64s
func sortInt64s(s []int64) {
	if len(s) < 2 {
		return
	}
	sort.Sort(int64Slice(s))
}

// int64Slice implements the sort.Interface for a slice of int64s
type int64Slice []int64

func (s int64Slice) Len() int           { return len(s) }
func (s int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// dedupInt64s deduplicates a given int64 slice which is already sorted.
func dedupInt64s(s []int64) []int64 {
	p := len(s) - 1
	if p <= 0 {
		return s
	}

	for i := p - 1; i >= 0; i-- {
		if s[p] != s[i] {
			p--
			s[p] = s[i]
		}
	}

	return s[p:]
}

// dedupStrings deduplicates a given string slice which is already sorted.
func dedupStrings(s []string) []string {
	p := len(s) - 1
	if p <= 0 {
		return s
	}

	for i := p - 1; i >= 0; i-- {
		if s[p] != s[i] {
			p--
			s[p] = s[i]
		}
	}

	return s[p:]
}

// a slightly expensive helper function which allows
// us to test if an interface value is nil or not
func isInterfaceValueNil(v interface{}) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}
