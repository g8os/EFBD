package storage

import (
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

// NonDedupedVdiskExists returns if the non deduped vdisk in question
// exists in the given ardb storage cluster.
func NonDedupedVdiskExists(vdiskID string, cluster config.StorageClusterConfig) (bool, error) {
	// storage key used on all data servers for this vdisk
	key := nonDedupedStorageKey(vdiskID)

	// go through each server to check if the vdisKID exists there
	// the first vdisk which has data for this vdisk,
	// we'll take as a sign that the vdisk exists
	for _, serverConfig := range cluster.Servers {
		exists, err := nonDedupedVdiskExistsOnServer(key, serverConfig)
		if exists || err != nil {
			return exists, err
		}
	}

	// no errors occured, but no server had the given storage key,
	// which means the vdisk doesn't exist
	return false, nil
}

func nonDedupedVdiskExistsOnServer(key string, server config.StorageServerConfig) (bool, error) {
	conn, err := ardb.Dial(server)
	if err != nil {
		return false, fmt.Errorf(
			"couldn't connect to data ardb %s@%d: %s",
			server.Address, server.Database, err.Error())
	}
	defer conn.Close()
	return ardb.Bool(conn.Do("EXISTS", key))
}

// ListNonDedupedBlockIndices returns all indices stored for the given nondeduped storage.
// This function will always either return an error OR indices.
// If this function returns indices, they are guaranteed to be in order from smallest to biggest.
func ListNonDedupedBlockIndices(vdiskID string, cluster config.StorageClusterConfig) ([]int64, error) {
	key := nonDedupedStorageKey(vdiskID)

	var indices []int64
	// collect the indices found on each data server
	for _, serverConfig := range cluster.Servers {
		serverIndices, err := listNonDedupedBlockIndicesOnDataServer(key, serverConfig)
		if err == ardb.ErrNil {
			log.Infof(
				"ardb server %s@%d doesn't contain any data for nondeduped vdisk %s",
				serverConfig.Address, serverConfig.Database, vdiskID)
			continue // it's ok if a server doesn't have anything stored
			// even though this might indicate a problem
			// in our sharding algorithm
		}
		if err != nil {
			return nil, err
		}
		// add it to the list of indices already found
		indices = append(indices, serverIndices...)
	}

	// if no indices could be found, we concider that as an error
	if len(indices) == 0 {
		return nil, ardb.ErrNil
	}

	sortInt64s(indices)
	return indices, nil
}

func listNonDedupedBlockIndicesOnDataServer(key string, server config.StorageServerConfig) ([]int64, error) {
	conn, err := ardb.Dial(server)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer conn.Close()
	return ardb.Int64s(conn.Do("HKEYS", key))
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

func newDeleteNonDedupedDataOp(vdiskID string) storageOp {
	return &deleteNonDedupedDataOp{vdiskID}
}

type deleteNonDedupedDataOp struct {
	vdiskID string
}

func (op *deleteNonDedupedDataOp) Send(sender storageOpSender) error {
	log.Debugf("batch deletion of nondeduped data for: %v", op.vdiskID)
	return sender.Send("DEL", nonDedupedStorageKey(op.vdiskID))
}

func (op *deleteNonDedupedDataOp) Receive(receiver storageOpReceiver) error {
	return ardb.Error(receiver.Receive())
}

func (op *deleteNonDedupedDataOp) Label() string {
	return "delete nondeduped data of " + op.vdiskID
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
