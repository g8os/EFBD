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
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ss.vdiskID, err)
			// broadcast the connection issue to 0-Orchestrator
			cfg := conn.ConnectionConfig()
			log.Broadcast(
				status,
				log.SubjectStorage,
				log.ARDBServerTimeoutBody{
					Address:  cfg.Address,
					Database: cfg.Database,
					Type:     log.ARDBPrimaryServer,
					VdiskID:  ss.vdiskID,
				},
			)
			// disable data connection,
			// so the server remains disabled until next config reload.
			ss.provider.DisableDataConnection(conn.ServerIndex())
		}
		return
	}
	defer conn.Close()

	// don't store zero blocks,
	// and delete existing ones if they already existed
	if ss.isZeroContent(content) {
		_, err = conn.Do("HDEL", ss.storageKey, blockIndex)
	} else {
		// content is not zero, so let's (over)write it
		_, err = conn.Do("HSET", ss.storageKey, blockIndex, content)
	}

	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ss.vdiskID, err)
			// broadcast the connection issue to 0-Orchestrator
			cfg := conn.ConnectionConfig()
			log.Broadcast(
				status,
				log.SubjectStorage,
				log.ARDBServerTimeoutBody{
					Address:  cfg.Address,
					Database: cfg.Database,
					Type:     log.ARDBPrimaryServer,
					VdiskID:  ss.vdiskID,
				},
			)
			// disable data connection,
			// so the server remains disabled until next config reload.
			ss.provider.DisableDataConnection(conn.ServerIndex())
		}
	}

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
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ss.vdiskID, err)
			// broadcast the connection issue to 0-Orchestrator
			cfg := conn.ConnectionConfig()
			log.Broadcast(
				status,
				log.SubjectStorage,
				log.ARDBServerTimeoutBody{
					Address:  cfg.Address,
					Database: cfg.Database,
					Type:     log.ARDBPrimaryServer,
					VdiskID:  ss.vdiskID,
				},
			)
			// disable data connection,
			// so the server remains disabled until next config reload.
			ss.provider.DisableDataConnection(conn.ServerIndex())
		}
		return
	}
	defer conn.Close()

	// get block from primary data storage server, if it exists at all,
	// a nil block is returned in case it didn't exist
	content, err = ardb.RedisBytes(conn.Do("HGET", ss.storageKey, blockIndex))
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ss.vdiskID, err)
			// broadcast the connection issue to 0-Orchestrator
			cfg := conn.ConnectionConfig()
			log.Broadcast(
				status,
				log.SubjectStorage,
				log.ARDBServerTimeoutBody{
					Address:  cfg.Address,
					Database: cfg.Database,
					Type:     log.ARDBPrimaryServer,
					VdiskID:  ss.vdiskID,
				},
			)
			// disable data connection,
			// so the server remains disabled until next config reload.
			ss.provider.DisableDataConnection(conn.ServerIndex())
		}
	}
	return
}

// (*nonDedupedStorage).getContent in case storage has template support
func (ss *nonDedupedStorage) getPrimaryOrTemplateContent(blockIndex int64) (content []byte, err error) {
	content, err = ss.getPrimaryContent(blockIndex)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	content, err = func() (content []byte, err error) {
		// get a connection to a template data storage server, based on the modulo blockIndex
		conn, err := ss.provider.TemplateConnection(blockIndex)
		if err != nil {
			if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
				log.Errorf("template server network error for vdisk %s: %v", ss.vdiskID, err)
				// broadcast the connection issue to 0-Orchestrator
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBTemplateServer,
						VdiskID:  ss.vdiskID,
					},
				)
				// disable template connection,
				// so the server remains disabled until next config reload.
				ss.provider.DisableTemplateConnection(conn.ServerIndex())
			} else if err == ardb.ErrTemplateClusterNotSpecified {
				err = nil
			}
			return
		}
		defer conn.Close()

		// get block from template data storage server, if it exists at all,
		// a nil block is returned in case it didn't exist
		content, err = ardb.RedisBytes(conn.Do("HGET", ss.templateStorageKey, blockIndex))
		if err != nil {
			content = nil
			if err == redis.ErrNil {
				log.Debugf(
					"content for block %d (vdisk %s) not available in primary-, nor in template storage: %s",
					blockIndex, ss.templateVdiskID, err.Error())
			} else if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
				log.Errorf("template server network error for vdisk %s: %v", ss.vdiskID, err)
				// broadcast the connection issue to 0-Orchestrator
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBTemplateServer,
						VdiskID:  ss.vdiskID,
					},
				)
				// disable template connection,
				// so the server remains disabled until next config reload.
				ss.provider.DisableTemplateConnection(conn.ServerIndex())
			}
		}

		return
	}()
	if err != nil || content == nil {
		return
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
func NonDedupedVdiskExists(vdiskID string, cluster *config.StorageClusterConfig) (bool, error) {
	if cluster == nil {
		return false, errors.New("no cluster config given")
	}

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
	conn, err := ardb.GetConnection(server)
	if err != nil {
		return false, fmt.Errorf(
			"couldn't connect to data ardb %s@%d: %s",
			server.Address, server.Database, err.Error())
	}
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}

// ListNonDedupedBlockIndices returns all indices stored for the given nondeduped storage.
// This function will always either return an error OR indices.
// If this function returns indices, they are guaranteed to be in order from smallest to biggest.
func ListNonDedupedBlockIndices(vdiskID string, cluster *config.StorageClusterConfig) ([]int64, error) {
	if cluster == nil {
		return nil, errors.New("no cluster config given")
	}

	key := nonDedupedStorageKey(vdiskID)

	var indices []int64
	// collect the indices found on each data server
	for _, serverConfig := range cluster.Servers {
		serverIndices, err := listNonDedupedBlockIndicesOnDataServer(key, serverConfig)
		if err == redis.ErrNil {
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
		return nil, redis.ErrNil
	}

	sortInt64s(indices)
	return indices, nil
}

func listNonDedupedBlockIndicesOnDataServer(key string, server config.StorageServerConfig) ([]int64, error) {
	conn, err := ardb.GetConnection(server)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer conn.Close()
	return ardb.RedisInt64s(conn.Do("HKEYS", key))
}

// CopyNonDeduped copies a non-deduped storage
// within the same or between different storage clusters.
func CopyNonDeduped(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// validate source cluster
	if sourceCluster == nil {
		return errors.New("no source cluster given")
	}
	sourceDataServerCount := len(sourceCluster.Servers)
	if sourceDataServerCount == 0 {
		return errors.New("no data server configs given for source")
	}

	// define whether or not we're copying between different clusters,
	// and if the target cluster is given, make sure to validate it.
	if targetCluster == nil {
		targetCluster = sourceCluster
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

		if sourceCfg.Equal(&targetCfg) {
			// within same storage server
			err = func() error {
				conn, err := ardb.GetConnection(sourceCfg)
				if err != nil {
					return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
				}
				defer conn.Close()

				return copyNonDedupedSameConnection(sourceID, targetID, conn)
			}()
		} else {
			// between different storage servers
			err = func() error {
				conns, err := ardb.GetConnections(sourceCfg, targetCfg)
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

func copyNonDedupedSameConnection(sourceID, targetID string, conn redis.Conn) (err error) {
	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	sourceKey := nonDedupedStorageKey(sourceID)
	targetKey := nonDedupedStorageKey(targetID)

	indexCount, err := redis.Int64(copyNonDedupedSameConnScript.Do(conn, sourceKey, targetKey))
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
