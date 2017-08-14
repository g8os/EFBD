package storage

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
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

	if cfg.BlockSize <= 0 {
		return errors.New("invalid block size size")
	}

	return nil
}

// NewBlockStorage returns the correct block storage based on the given VdiskConfig.
func NewBlockStorage(cfg BlockStorageConfig, provider ardb.ConnProvider) (storage BlockStorage, err error) {
	err = cfg.Validate()
	if err != nil {
		return
	}

	vdiskType := cfg.VdiskType

	switch storageType := vdiskType.StorageType(); storageType {
	case config.StorageDeduped:
		return Deduped(
			cfg.VdiskID,
			cfg.BlockSize,
			cfg.LBACacheLimit,
			vdiskType.TemplateSupport(),
			provider)

	case config.StorageNonDeduped:
		return NonDeduped(
			cfg.VdiskID,
			cfg.TemplateVdiskID,
			cfg.BlockSize,
			vdiskType.TemplateSupport(),
			provider)

	case config.StorageSemiDeduped:
		return SemiDeduped(
			cfg.VdiskID,
			cfg.BlockSize,
			cfg.LBACacheLimit,
			provider)

	default:
		return nil, fmt.Errorf(
			"no block storage available for %s's storage type %s",
			cfg.VdiskID, storageType)
	}
}

// VdiskExists returns true if the vdisk question exists in the given ardb storage cluster.
// An error is returned in case this couldn't be verified for whatever reason.
func VdiskExists(id string, t config.VdiskType, ccfg *config.StorageClusterConfig) (bool, error) {
	switch st := t.StorageType(); st {
	case config.StorageDeduped:
		return DedupedVdiskExists(id, ccfg)

	case config.StorageNonDeduped:
		return NonDedupedVdiskExists(id, ccfg)

	case config.StorageSemiDeduped:
		return SemiDedupedVdiskExists(id, ccfg)

	default:
		return false, fmt.Errorf("%v is not a supported storage type", st)
	}
}

// ListBlockIndices returns all indices stored for the given storage.
// This function will always either return an error OR indices.
func ListBlockIndices(id string, t config.VdiskType, ccfg *config.StorageClusterConfig) ([]int64, error) {
	switch st := t.StorageType(); st {
	case config.StorageDeduped:
		return ListDedupedBlockIndices(id, ccfg)

	case config.StorageNonDeduped:
		return ListNonDedupedBlockIndices(id, ccfg)

	case config.StorageSemiDeduped:
		return ListSemiDedupedBlockIndices(id, ccfg)

	default:
		return nil, fmt.Errorf("%v is not a supported storage type", st)
	}
}

// ScanForAvailableVdisks scans a given storage servers
// for available vdisks, and returns their ids.
func ScanForAvailableVdisks(cfg config.StorageServerConfig) ([]string, error) {
	log.Debugf("connection to ardb at %s (db: %d)",
		cfg.Address, cfg.Database)
	conn, err := ardb.GetConnection(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to the ardb: %s", err.Error())
	}
	defer conn.Close()

	log.Debugf("scanning for all available vdisks...")

	const (
		startListCursor       = "0"
		vdiskListScriptSource = `
	local cursor = ARGV[1]

local result = redis.call("SCAN", cursor)
local batch = result[2]

local key
local type

local output = {}

for i = 1, #batch do
	key = batch[i]

	-- only add hashmaps
	type = redis.call("TYPE", key)
	type = type.ok or type
	if type == "hash" then
		table.insert(output, key)
	end
end

cursor = result[1]
table.insert(output, cursor)

return output
`
	)

	script := redis.NewScript(0, vdiskListScriptSource)
	cursor := startListCursor
	var output []string

	var vdisks []string
	var vdisksLength int

	// go through all available keys
	for {
		output, err = redis.Strings(script.Do(conn, cursor))
		if err != nil {
			log.Error("aborting key scan due to an error: ", err)
			break
		}

		vdisksLength = len(output) - 1
		if vdisksLength > 0 {
			vdisks = append(vdisks, output[:vdisksLength]...)
		}

		cursor = output[vdisksLength]
		if startListCursor == cursor {
			break
		}
	}

	if len(vdisks) == 0 {
		return nil, nil
	}

	var ok bool
	var vdiskID string

	// only log each vdisk once
	uniqueVdisks := make(map[string]struct{})
	for i := len(vdisks) - 1; i >= 0; i-- {
		vdiskID = filterListedVdiskID(string(vdisks[i]))
		if vdiskID != "" {
			if _, ok = uniqueVdisks[vdiskID]; !ok {
				// if vdisk is valid and unique
				// don't delete it
				continue
			}
		}

		// add vdisk to unique vdisks map
		uniqueVdisks[vdiskID] = struct{}{}

		// delete vdisk
		vdisks[i] = vdisks[len(vdisks)-1]
		vdisks = vdisks[:len(vdisks)-1]
	}

	return vdisks, nil
}

// filterListedVdiskID only accepts keys with a known prefix,
// if no known prefix is found an empty string is returned,
// otherwise the prefix is removed and the vdiskID is returned.
func filterListedVdiskID(key string) string {
	parts := storageKeyPrefixRex.FindStringSubmatch(key)
	if len(parts) == 3 {
		return parts[2]
	}

	return ""
}

var storageKeyPrefixRex = regexp.MustCompile("^(" +
	strings.Join(knownStorageKeyPrefixes, "|") +
	")(.+)$")

var knownStorageKeyPrefixes = []string{
	lba.StorageKeyPrefix,
	nonDedupedStorageKeyPrefix,
	semiDedupBitMapKeyPrefix,
}

// DeleteMetadata deletes all metadata for the given vdisks from the given storage server.
func DeleteMetadata(cfg config.StorageServerConfig, vdisks map[string]config.VdiskType) error {
	var pipeline storageOpPipeline

	for vdiskID, vdiskType := range vdisks {
		switch vdiskType.StorageType() {
		case config.StorageDeduped:
			pipeline.Add(newDeleteDedupedMetadataOp(vdiskID))
		case config.StorageSemiDeduped:
			pipeline.Add(newDeleteSemiDedupedMetaDataOp(vdiskID))
		}
	}

	return pipeline.Apply(cfg)
}

// DeleteData deletes all data for the given vdisks from the given storage server.
func DeleteData(cfg config.StorageServerConfig, vdisks map[string]config.VdiskType) error {
	var pipeline storageOpPipeline

	for vdiskID, vdiskType := range vdisks {
		switch vdiskType.StorageType() {
		case config.StorageNonDeduped:
			pipeline.Add(newDeleteNonDedupedDataOp(vdiskID))
		case config.StorageSemiDeduped:
			pipeline.Add(newDeleteSemiDedupedDataOp(vdiskID))
		}
	}

	return pipeline.Apply(cfg)
}

// storageOpSender is used to send (see: batch) commands and their arguments,
// such that a group of commands (see: transaction) can be applied all together.
type storageOpSender interface {
	Send(commandName string, args ...interface{}) error
}

// storageOpSender is used to receive and decode a reply from
// the ARDB server, such that a command applied can be validated.
type storageOpReceiver interface {
	Receive() (reply interface{}, err error)
}

// storageOp defines the interface for any kind of operation
// that we wish to apply directly onto the ARDB
type storageOp interface {
	Send(storageOpSender) error
	Receive(storageOpReceiver) error
	Label() string
}

// storageOpPipeline is simply a group of operations
// that can be applied all together (and as many times as you want)
// to an(y) ARDB server.
type storageOpPipeline []storageOp

// Add an operation so it can be applied later.
func (ops *storageOpPipeline) Add(op storageOp) {
	if op == nil {
		return
	}

	*ops = append(*ops, op)
}

// Remove all earlier added operations from this pipeline.
func (ops *storageOpPipeline) Clear() {
	*ops = nil
}

// Apply all added operations to the given ARDB server.
func (ops storageOpPipeline) Apply(cfg config.StorageServerConfig) error {
	if ops == nil {
		return nil
	}

	conn, err := ardb.GetConnection(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, op := range ops {
		err = op.Send(conn)
		if err != nil {
			return fmt.Errorf(
				"couldn't batch pipeline op '%s': %v", op.Label(), err)
		}
	}

	err = conn.Flush()
	if err != nil {
		return fmt.Errorf(
			"couldn't apply the %d pipelined operations", len(ops))
	}

	var errs pipelineErrors

	for _, op := range ops {
		err = op.Receive(conn)
		errs.AddErrorMsg(err, "op '%s' failed", op.Label())
	}

	if errs != nil {
		return errs
	}

	return nil
}

// pipelineErrors is a nice utility type,
// which allows for collecting multiple errors
// and returning them as a single error.
//
// NOTE: might be nice to turn this into a
// general purpose type we use throughout the 0-Disk codebase
// or perhaps there is a nice lib that already does this for us,
// that we should use instead of this.
// Either way, it works for now (and is unit tested).
//
// WARNING: when returning it as an `error` interface,
// make sure to check first if pipelinErrors is not `nil`,
// because once you turn it into an interface (`error`),
// it won't be `nil` any longer, even though its concrete type is `nil`.
// This is because of how interfaces are implemented in Go,
// so be careful for that. You have been warned!
type pipelineErrors []error

// Add an error (if it's not nil) to the slice of errors.
func (errs *pipelineErrors) AddError(err error) {
	if err == nil {
		return
	}

	*errs = append(*errs, err)
}

// Add an error (if it's not nil) to the slice of errors,
// and preprend it with a formatted message (if given).
func (errs *pipelineErrors) AddErrorMsg(err error, format string, args ...interface{}) {
	if err == nil {
		return
	}

	if format == "" {
		*errs = append(*errs, err)
		return
	}

	*errs = append(*errs,
		fmt.Errorf(format+" (%v)", append(args, err)...))
}

// Turn the slice of errors into a single error string.
// If the slice is empty, an empty string will be returned instead.
func (errs pipelineErrors) Error() string {
	if len(errs) == 0 {
		return ""
	}

	var str string
	for _, err := range errs {
		str += err.Error() + ", "
	}

	return str[:len(str)-2]
}

// sortInt64s sorts a slice of int64s
func sortInt64s(s []int64) {
	sort.Sort(int64Slice(s))
}

// int64Slice implements the sort.Interface for a slice of int64s
type int64Slice []int64

func (s int64Slice) Len() int           { return len(s) }
func (s int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// dedupInt64s deduplicates a given int64 slice which is already sorted.
func dedupInt64s(s []int64) []int64 {
	for i, n := 0, len(s)-1; i < n; {
		if s[i] == s[i+1] {
			s = append(s[:i], s[i+1:]...)
			continue
		}
		i++
	}

	return s
}
