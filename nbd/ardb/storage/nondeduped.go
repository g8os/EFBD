package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// NonDeduped returns a non deduped BlockStorage
func NonDeduped(vdiskID, templateVdiskID string, blockSize int64, cluster, templateCluster ardb.StorageCluster) (BlockStorage, error) {
	// create the nondeduped storage with the info we know for sure
	nondeduped := &nonDedupedStorage{
		blockSize:       blockSize,
		storageKey:      nonDedupedStorageKey(vdiskID),
		vdiskID:         vdiskID,
		templateVdiskID: templateVdiskID,
		cluster:         cluster,
	}

	// define the getContent logic, based on whether or not we support a template cluster
	if isInterfaceValueNil(templateCluster) {
		nondeduped.getContent = nondeduped.getPrimaryContent
	} else {
		nondeduped.getContent = nondeduped.getPrimaryOrTemplateContent
		if templateVdiskID == "" {
			nondeduped.templateVdiskID = vdiskID
		}
		nondeduped.templateStorageKey = nonDedupedStorageKey(nondeduped.templateVdiskID)
		nondeduped.templateCluster = templateCluster
	}

	return nondeduped, nil
}

// nonDedupedStorage is a BlockStorage implementation,
// which simply stores each block in redis using
// a unique key based on the vdiskID and blockIndex.
type nonDedupedStorage struct {
	blockSize          int64                   // blocksize in bytes
	storageKey         string                  // Storage Key based on vdiskID
	templateStorageKey string                  // Storage Key based on templateVdiskID
	vdiskID            string                  // ID for the vdisk
	templateVdiskID    string                  // used in case template is supposed (same value as vdiskID if not defined)
	cluster            ardb.StorageCluster     // used to interact with the ARDB (StorageEngine) Cluster
	templateCluster    ardb.StorageCluster     // used to interact with the ARDB (StorageEngine) Template Cluster
	getContent         nondedupedContentGetter // getter depends on whether there is template support or not
}

// used to provide different content getters based on the vdisk properties
// it boils down to the question: does it have template support?
type nondedupedContentGetter func(blockIndex int64) (content []byte, err error)

// Set implements BlockStorage.Set
func (ss *nonDedupedStorage) SetBlock(blockIndex int64, content []byte) error {
	var cmd *ardb.StorageCommand

	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ss.isZeroContent(content) {
		cmd = ardb.Command(command.HashDelete, ss.storageKey, blockIndex)
	} else {
		// content is not zero, so let's (over)write it
		cmd = ardb.Command(command.HashSet, ss.storageKey, blockIndex, content)
	}

	return ardb.Error(ss.cluster.DoFor(blockIndex, cmd))
}

// Get implements BlockStorage.Get
func (ss *nonDedupedStorage) GetBlock(blockIndex int64) (content []byte, err error) {
	content, err = ss.getContent(blockIndex)
	return
}

// Delete implements BlockStorage.Delete
func (ss *nonDedupedStorage) DeleteBlock(blockIndex int64) error {
	cmd := ardb.Command(command.HashDelete, ss.storageKey, blockIndex)
	// delete the block defined for the block index (if it previously existed at all)
	return ardb.Error(ss.cluster.DoFor(blockIndex, cmd))
}

// Flush implements BlockStorage.Flush
func (ss *nonDedupedStorage) Flush() (err error) {
	// nothing to do for the nonDeduped BlockStorage
	return
}

// Close implements BlockStorage.Close
func (ss *nonDedupedStorage) Close() error { return nil }

// (*nonDedupedStorage).getContent in case storage has no template support
func (ss *nonDedupedStorage) getPrimaryContent(blockIndex int64) (content []byte, err error) {
	cmd := ardb.Command(command.HashGet, ss.storageKey, blockIndex)
	return ardb.OptBytes(ss.cluster.DoFor(blockIndex, cmd))
}

// (*nonDedupedStorage).getContent in case storage has template support
func (ss *nonDedupedStorage) getPrimaryOrTemplateContent(blockIndex int64) (content []byte, err error) {
	content, err = ss.getPrimaryContent(blockIndex)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	cmd := ardb.Command(command.HashGet, ss.storageKey, blockIndex)
	content, err = ardb.OptBytes(ss.templateCluster.DoFor(blockIndex, cmd))
	if err != nil {
		// this error is returned, in case the cluster is simply not defined,
		// which is an error we'll ignore, as it means we cannot use the template cluster,
		// and thus no content is returned, and neither an error.
		if err == ErrClusterNotDefined {
			return nil, nil
		}
		// no content to return,
		// exit with an error
		return nil, err
	}
	if content == nil {
		// no content or error to return
		return nil, nil
	}

	// check if we found the content in the template server
	// store template content in primary storage asynchronously
	go func() {
		err := ss.SetBlock(blockIndex, content)
		if err != nil {
			// we won't return error however, but just log it
			log.Infof(
				"couldn't store template content block %d in primary storage: %s",
				blockIndex, err.Error())
		}
	}()

	log.Debugf(
		"block %d not available in primary storage, but did find it in template storage",
		blockIndex)

	return
}

// isZeroContent detects if a given content buffer is completely filled with 0s
func (ss *nonDedupedStorage) isZeroContent(content []byte) bool {
	for _, c := range content {
		if c != 0 {
			return false
		}
	}

	return true
}

// nonDedupedVdiskExists checks if a non-deduped vdisks exists on a given cluster
func nonDedupedVdiskExists(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverCh, err := cluster.ServerIterator(ctx)
	if err != nil {
		return false, err
	}

	type serverResult struct {
		exists bool
		err    error
	}
	resultCh := make(chan serverResult)

	var count int
	action := ardb.Command(command.Exists, nonDedupedStorageKey(vdiskID))
	for server := range serverCh {
		server := server
		go func() {
			var result serverResult
			result.exists, result.err = ardb.Bool(server.Do(action))
			select {
			case resultCh <- result:
			case <-ctx.Done():
			}
		}()
		count++
	}

	var result serverResult
	for i := 0; i < count; i++ {
		result = <-resultCh
		if result.exists || result.err != nil {
			return result.exists, result.err
		}
	}

	return false, nil
}

// deleteNonDedupedData deletes the non-deduped data of a given vdisk from a given cluster.
func deleteNonDedupedData(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverCh, err := cluster.ServerIterator(ctx)
	if err != nil {
		return false, err
	}

	type serverResult struct {
		count int
		err   error
	}
	resultCh := make(chan serverResult)

	var serverCount int
	action := ardb.Command(command.Delete, nonDedupedStorageKey(vdiskID))
	for server := range serverCh {
		server := server
		go func() {
			var result serverResult
			result.count, result.err = ardb.Int(server.Do(action))
			select {
			case resultCh <- result:
			case <-ctx.Done():
			}
		}()
		serverCount++
	}

	var deleteCount int
	var result serverResult
	for i := 0; i < serverCount; i++ {
		result = <-resultCh
		if result.err != nil {
			return false, result.err
		}
		deleteCount += result.count
	}
	return deleteCount > 0, nil
}

// listNonDedupedBlockIndices lists all the block indices (sorted)
// from a non-deduped vdisk stored on a given cluster.
func listNonDedupedBlockIndices(vdiskID string, cluster ardb.StorageCluster) ([]int64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverCh, err := cluster.ServerIterator(ctx)
	if err != nil {
		return nil, err
	}

	type serverResult struct {
		indices []int64
		err     error
	}
	resultCh := make(chan serverResult)

	var serverCount int
	action := ardb.Command(command.HashKeys, nonDedupedStorageKey(vdiskID))
	for server := range serverCh {
		server := server
		go func() {
			var result serverResult
			result.indices, result.err = ardb.Int64s(server.Do(action))
			if result.err == ardb.ErrNil {
				result.err = nil
			}
			select {
			case resultCh <- result:
			case <-ctx.Done():
			}
		}()
		serverCount++
	}

	var indices []int64
	var result serverResult
	for i := 0; i < serverCount; i++ {
		result = <-resultCh
		if result.err != nil {
			return nil, result.err
		}
		indices = append(indices, result.indices...)
	}

	sortInt64s(indices)
	return indices, nil
}

// CopyNonDeduped copies a non-deduped storage
// within the same or between different storage clusters.
func CopyNonDeduped(sourceID, targetID string, sourceCluster config.StorageClusterConfig, targetCluster *config.StorageClusterConfig) error {
	sourceDataServerCount := len(sourceCluster.Servers)
	if sourceDataServerCount == 0 {
		return errors.New("no data server configs given for source")
	}

	// define whether or not we're copying between different clusters,
	// and if the target cluster is given, make sure to validate it.
	if targetCluster == nil {
		targetCluster = &sourceCluster
	} else {
		targetDataServerCount := len(targetCluster.Servers)
		// [TODO]
		// Currently the result will be WRONG in case targetDataServerCount != sourceDataServerCount,
		// as the storage data spread will not be the same,
		// to what the nbdserver read calls will expect.
		// See open issue for more information:
		// https://github.com/zero-os/0-Disk/issues/206
		if targetDataServerCount != sourceDataServerCount {
			return errors.New("target data server count has to equal the source data server count")
		}
	}

	var err error
	var sourceCfg, targetCfg config.StorageServerConfig

	for i := 0; i < sourceDataServerCount; i++ {
		sourceCfg = sourceCluster.Servers[i]
		targetCfg = targetCluster.Servers[i]

		if sourceCfg.Equal(targetCfg) {
			// within same storage server
			err = func() error {
				conn, err := ardb.Dial(sourceCfg)
				if err != nil {
					return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
				}
				defer conn.Close()

				return copyNonDedupedSameConnection(sourceID, targetID, conn)
			}()
		} else {
			// between different storage servers
			err = func() error {
				conns, err := ardb.DialAll(sourceCfg, targetCfg)
				if err != nil {
					return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
				}
				defer func() {
					conns[0].Close()
					conns[1].Close()
				}()

				return copyNonDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
			}()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func copyNonDedupedSameConnection(sourceID, targetID string, conn ardb.Conn) (err error) {
	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	sourceKey := nonDedupedStorageKey(sourceID)
	targetKey := nonDedupedStorageKey(targetID)

	indexCount, err := ardb.Int64(copyNonDedupedSameConnScript.Do(conn, sourceKey, targetKey))
	if err == nil {
		log.Infof("copied %d block indices to vdisk %q",
			indexCount, targetID)
	}

	return
}

func copyNonDedupedDifferentConnections(sourceID, targetID string, connA, connB ardb.Conn) (err error) {
	sourceKey := nonDedupedStorageKey(sourceID)
	targetKey := nonDedupedStorageKey(targetID)

	// get data from source connection
	log.Infof("collecting all data from source vdisk %q...", sourceID)
	data, err := redis.StringMap(connA.Do("HGETALL", sourceKey))
	if err != nil {
		return

	}
	if len(data) == 0 {
		err = fmt.Errorf("%q does not exist", sourceID)
		return
	}
	log.Infof("collected %d block indices from source vdisk %q",
		len(data), sourceID)

	// start the copy transaction
	if err = connB.Send("MULTI"); err != nil {
		return
	}

	// delete any existing vdisk
	if err = connB.Send("DEL", targetKey); err != nil {
		return
	}

	// buffer all data on target connection
	log.Infof("buffering %d block indices for target vdisk %q...",
		len(data), targetID)
	var index int64
	for rawIndex, content := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", targetKey, index, []byte(content))
	}

	// send all data to target connection (execute the transaction)
	log.Infof("flushing buffered data for target vdisk %q...", targetID)
	response, err := connB.Do("EXEC")
	if err == nil && response == nil {
		// if response == <nil> the transaction has failed
		// more info: https://redis.io/topics/transactions
		err = fmt.Errorf("vdisk %q was busy and couldn't be modified", targetID)
	}

	return
}

// nonDedupedStorageKey returns the storage key that can/will be
// used to store the nondeduped data for the given vdiskID
func nonDedupedStorageKey(vdiskID string) string {
	return nonDedupedStorageKeyPrefix + vdiskID
}

const (
	// nonDedupedStorageKeyPrefix is the prefix used in nonDedupedStorageKey
	nonDedupedStorageKeyPrefix = "nondedup:"
)

var copyNonDedupedSameConnScript = redis.NewScript(0, `
local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", source) == 0 then
    return redis.error_reply('"' .. source .. '" does not exist')
end

if redis.call("EXISTS", destination) == 1 then
    redis.call("DEL", destination)
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))

return redis.call("HLEN", destination)
`)
