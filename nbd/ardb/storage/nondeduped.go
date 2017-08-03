package storage

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// NonDeduped returns a non deduped BlockStorage
func NonDeduped(vdiskID, templateVdiskID string, blockSize int64, templateSupport bool, provider ardb.ConnProvider) (BlockStorage, error) {
	// create the nondeduped storage with the info we know for sure
	nondeduped := &nonDedupedStorage{
		blockSize:       blockSize,
		storageKey:      nonDedupedStorageKey(vdiskID),
		vdiskID:         vdiskID,
		templateVdiskID: templateVdiskID,
		provider:        provider,
	}

	// define the getContent logic, based on whether or not we support a template cluster
	if templateSupport {
		nondeduped.getContent = nondeduped.getPrimaryOrTemplateContent
		if templateVdiskID == "" {
			nondeduped.templateVdiskID = vdiskID
		}
		nondeduped.templateStorageKey = nonDedupedStorageKey(nondeduped.templateVdiskID)
	} else {
		nondeduped.getContent = nondeduped.getPrimaryContent
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
	provider           ardb.ConnProvider       // used to get the connection info to storage servers
	getContent         nondedupedContentGetter // getter depends on whether there is template support or not
}

// used to provide different content getters based on the vdisk properties
// it boils down to the question: does it have template support?
type nondedupedContentGetter func(blockIndex int64) (content []byte, err error)

// Set implements BlockStorage.Set
func (ss *nonDedupedStorage) SetBlock(blockIndex int64, content []byte) (err error) {
	// get a connection to a data storage server, based on the modulo blockIndex
	conn, err := ss.provider.DataConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ss.isZeroContent(content) {
		_, err = conn.Do("HDEL", ss.storageKey, blockIndex)
		return
	}

	// content is not zero, so let's (over)write it
	_, err = conn.Do("HSET", ss.storageKey, blockIndex, content)
	return
}

// Get implements BlockStorage.Get
func (ss *nonDedupedStorage) GetBlock(blockIndex int64) (content []byte, err error) {
	content, err = ss.getContent(blockIndex)
	return
}

// Delete implements BlockStorage.Delete
func (ss *nonDedupedStorage) DeleteBlock(blockIndex int64) (err error) {
	// get a connection to a data storage server, based on the modulo blockIndex
	conn, err := ss.provider.DataConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// delete the block defined for the block index (if it previously existed at all)
	_, err = conn.Do("HDEL", ss.storageKey, blockIndex)
	return
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
	// get a connection to a data storage server, based on the modulo blockIndex
	conn, err := ss.provider.DataConnection(blockIndex)
	if err != nil {
		return
	}
	defer conn.Close()

	// get block from primary data storage server, if it exists at all,
	// a nil block is returned in case it didn't exist
	content, err = ardb.RedisBytes(conn.Do("HGET", ss.storageKey, blockIndex))
	return
}

// (*nonDedupedStorage).getContent in case storage has template support
func (ss *nonDedupedStorage) getPrimaryOrTemplateContent(blockIndex int64) (content []byte, err error) {
	content, err = ss.getPrimaryContent(blockIndex)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	content = func() (content []byte) {
		// get a connection to a template data storage server, based on the modulo blockIndex
		conn, err := ss.provider.TemplateConnection(blockIndex)
		if err != nil {
			return
		}
		defer conn.Close()

		// get block from template data storage server, if it exists at all,
		// a nil block is returned in case it didn't exist
		content, err = ardb.RedisBytes(conn.Do("HGET", ss.templateStorageKey, blockIndex))
		if err != nil {
			log.Debugf(
				"content for block %d (vdisk %s) not available in primary-, nor in template storage: %s",
				blockIndex, ss.templateVdiskID, err.Error())
			content = nil
		}

		return
	}()

	// check if we found the content in the template server
	if content != nil {
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
	}

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

// CopyNonDeduped copies a non-deduped storage
// within the same or between different storage clusters.
func CopyNonDeduped(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// validate source cluster
	if sourceCluster == nil {
		return errors.New("no source cluster given")
	}
	sourceDataServerCount := len(sourceCluster.DataStorage)
	if sourceDataServerCount == 0 {
		return errors.New("no data server configs given for source")
	}

	// define whether or not we're copying between different clusters,
	// and if the target cluster is given, make sure to validate it.
	if targetCluster == nil {
		targetCluster = sourceCluster
	} else {
		targetDataServerCount := len(targetCluster.DataStorage)
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

	var sourceCfg, targetCfg config.StorageServerConfig
	for i := 0; i < sourceDataServerCount; i++ {
		sourceCfg = sourceCluster.DataStorage[i]
		targetCfg = targetCluster.DataStorage[i]

		// within same storage server
		if sourceCfg == targetCfg {
			conn, err := ardb.GetConnection(sourceCfg)
			if err != nil {
				return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
			}
			defer conn.Close()

			return copyNonDedupedSameConnection(sourceID, targetID, conn)
		}

		// between different storage servers
		conns, err := ardb.GetConnections(sourceCfg, targetCfg)
		if err != nil {
			return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
		}
		defer func() {
			conns[0].Close()
			conns[1].Close()
		}()

		err = copyNonDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
		if err != nil {
			return err
		}
	}

	return nil
}

func copyNonDedupedSameConnection(sourceID, targetID string, conn redis.Conn) (err error) {
	script := redis.NewScript(0, `
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

	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	sourceKey := nonDedupedStorageKey(sourceID)
	targetKey := nonDedupedStorageKey(targetID)

	indexCount, err := redis.Int64(script.Do(conn, sourceKey, targetKey))
	if err == nil {
		log.Infof("copied %d block indices to vdisk %q",
			indexCount, targetID)
	}

	return
}

func copyNonDedupedDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) (err error) {
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
	_, err := receiver.Receive()
	return err
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
