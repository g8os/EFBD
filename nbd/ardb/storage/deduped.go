package storage

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
)

// Deduped returns a deduped BlockStorage
func Deduped(vdiskID string, blockSize, lbaCacheLimit int64, templateSupport bool, provider ardb.ConnProvider) (BlockStorage, error) {
	// define the LBA cache limit
	cacheLimit := lbaCacheLimit
	if cacheLimit < lba.BytesPerSector {
		log.Infof(
			"LBACacheLimit (%d) will be defaulted to %d (min-capped)",
			cacheLimit, lba.BytesPerSector)
		cacheLimit = ardb.DefaultLBACacheLimit
	}

	// create the LBA (used to store deduped metadata)
	vlba, err := lba.NewLBA(
		vdiskID,
		cacheLimit,
		provider,
	)
	if err != nil {
		log.Errorf("couldn't create the LBA: %s", err.Error())
		return nil, err
	}

	dedupedStorage := &dedupedStorage{
		blockSize:       blockSize,
		vdiskID:         vdiskID,
		zeroContentHash: zerodisk.HashBytes(make([]byte, blockSize)),
		provider:        provider,
		lba:             vlba,
	}

	// getContent is ALWAYS defined,
	// but the actual function used depends on
	// whether or not this storage has template support.
	if templateSupport {
		dedupedStorage.getContent = dedupedStorage.getPrimaryOrTemplateContent
	} else {
		dedupedStorage.getContent = dedupedStorage.getPrimaryContent
	}

	return dedupedStorage, nil
}

// dedupedStorage is a BlockStorage implementation,
// that stores the content (the data) based on a hash unique to that content,
// all hashes are linked to the vdisk using lba.LBA (the metadata).
// The metadata and data are stored on seperate servers.
// Accessing data is only ever possible by checking the metadata first.
type dedupedStorage struct {
	blockSize       int64                // block size in bytes
	vdiskID         string               // ID of the vdisk
	zeroContentHash zerodisk.Hash        // a hash of a nil-block of blockSize
	provider        ardb.ConnProvider    // used to get a connection to a storage server
	lba             *lba.LBA             // the LBA used to get/set/modify the metadata (content hashes)
	getContent      dedupedContentGetter // getContent function used to get content, is always defined
}

// used to provide different content getters based on the vdisk properties
// it boils down to the question: does it have template support?
type dedupedContentGetter func(hash zerodisk.Hash) (content []byte, err error)

// SetBlock implements BlockStorage.SetBlock
func (ds *dedupedStorage) SetBlock(blockIndex int64, content []byte) (err error) {
	hash := zerodisk.HashBytes(content)
	if ds.zeroContentHash.Equals(hash) {
		log.Debugf(
			"deleting hash @ %d from LBA for deduped vdisk %s as it's an all zeroes block",
			blockIndex, ds.vdiskID)
		err = ds.lba.Delete(blockIndex)
		return
	}

	// reference the content to this vdisk,
	// and set the content itself, if it didn't exist yet
	err = ds.setContent(hash, content)
	if err != nil {
		return
	}

	return ds.lba.Set(blockIndex, hash)
}

// GetBlock implements BlockStorage.GetBlock
func (ds *dedupedStorage) GetBlock(blockIndex int64) (content []byte, err error) {
	hash, err := ds.lba.Get(blockIndex)
	if err == nil && hash != nil && !hash.Equals(zerodisk.NilHash) {
		content, err = ds.getContent(hash)
	}
	return
}

// DeleteBlock implements BlockStorage.DeleteBlock
func (ds *dedupedStorage) DeleteBlock(blockIndex int64) (err error) {
	// first get hash
	hash, _ := ds.lba.Get(blockIndex)
	if hash == nil {
		// content didn't exist yet,
		// so we've nothing to do here
		return
	}

	// delete the actual hash from the LBA
	err = ds.lba.Delete(blockIndex)
	return
}

// Flush implements BlockStorage.Flush
func (ds *dedupedStorage) Flush() (err error) {
	err = ds.lba.Flush()
	return
}

func (ds *dedupedStorage) getDataConnection(hash zerodisk.Hash) (ardb.Connection, error) {
	return ds.provider.DataConnection(int64(hash[0]))
}

func (ds *dedupedStorage) getTemplateConnection(hash zerodisk.Hash) (ardb.Connection, error) {
	return ds.provider.TemplateConnection(int64(hash[0]))
}

// getPrimaryContent gets content from the primary storage.
// Assigned to (*dedupedStorage).getContent in case this storage has no template support.
func (ds *dedupedStorage) getPrimaryContent(hash zerodisk.Hash) (content []byte, err error) {
	conn, err := ds.getDataConnection(hash)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ds.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if ds.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  ds.vdiskID,
					},
				)
			}
		}
		return
	}
	defer conn.Close()

	content, err = ardb.RedisBytes(conn.Do("GET", hash.Bytes()))
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ds.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if ds.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  ds.vdiskID,
					},
				)
			}
		}
	}
	return
}

// getPrimaryOrTemplateContent gets content from the primary storage,
// or if the content can't be found in primary storage,
// we'll try to fetch it from the template storage.
// if the content is available in the template storage,
// we'll also try to store it in the primary storage before returning that content.
// Assigned to (*dedupedStorage).getContent in case this storage has template support.
func (ds *dedupedStorage) getPrimaryOrTemplateContent(hash zerodisk.Hash) (content []byte, err error) {
	// try to fetch it from the primary storage
	content, err = ds.getPrimaryContent(hash)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	// try to fetch it from the template storage if available
	content, err = func() (content []byte, err error) {
		conn, err := ds.getTemplateConnection(hash)
		if err != nil {
			log.Debugf(
				"content not available in primary storage for %v and no template storage available: %s",
				hash, err.Error())
			if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
				log.Errorf("template server network error for vdisk %s: %v", ds.vdiskID, err)
				// disable template data connection,
				// so the server remains disabled until next config reload.
				if ds.provider.DisableTemplateConnection(conn.ServerIndex()) {
					// only if the template data connection wasn't already disabled,
					// we'll broadcast the failure
					cfg := conn.ConnectionConfig()
					log.Broadcast(
						status,
						log.SubjectStorage,
						log.ARDBServerTimeoutBody{
							Address:  cfg.Address,
							Database: cfg.Database,
							Type:     log.ARDBTemplateServer,
							VdiskID:  ds.vdiskID,
						},
					)
				}
			} else if err == ardb.ErrTemplateClusterNotSpecified {
				err = nil
			}

			return
		}
		defer conn.Close()

		content, err = ardb.RedisBytes(conn.Do("GET", hash.Bytes()))
		if err != nil {
			content = nil
			if err == redis.ErrNil {
				log.Debugf(
					"content for %v not available in primary-, nor in template storage: %s",
					hash, err.Error())
			} else if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
				log.Errorf("template server network error for vdisk %s: %v", ds.vdiskID, err)
				// disable template data connection,
				// so the server remains disabled until next config reload.
				if ds.provider.DisableTemplateConnection(conn.ServerIndex()) {
					// only if the template data connection wasn't already disabled,
					// we'll broadcast the failure
					cfg := conn.ConnectionConfig()
					log.Broadcast(
						status,
						log.SubjectStorage,
						log.ARDBServerTimeoutBody{
							Address:  cfg.Address,
							Database: cfg.Database,
							Type:     log.ARDBTemplateServer,
							VdiskID:  ds.vdiskID,
						},
					)
				}
			}
		}

		return
	}()
	if content == nil || err != nil {
		return
	}

	// store template content in primary storage asynchronously
	go func() {
		err := ds.setContent(hash, content)
		if err != nil {
			// we won't return error however, but just log it
			log.Errorf("couldn't store template content in primary storage: %s", err.Error())
			return
		}

		log.Debugf(
			"stored template content for %v in primary storage (asynchronously)",
			hash)
	}()

	log.Debugf(
		"content not available in primary storage for %v, but did find it in template storage",
		hash)

	// err = nil, content != nil
	return
}

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(hash zerodisk.Hash, content []byte) error {
	conn, err := ds.getDataConnection(hash)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ds.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if ds.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  ds.vdiskID,
					},
				)
			}
		}
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", hash.Bytes(), content)
	if err != nil {
		if status, ok := ardb.MapErrorToBroadcastStatus(err); ok {
			log.Errorf("primary server network error for vdisk %s: %v", ds.vdiskID, err)
			// disable data connection,
			// so the server remains disabled until next config reload.
			if ds.provider.DisableDataConnection(conn.ServerIndex()) {
				// only if the data connection wasn't already disabled,
				// we'll broadcast the failure
				cfg := conn.ConnectionConfig()
				log.Broadcast(
					status,
					log.SubjectStorage,
					log.ARDBServerTimeoutBody{
						Address:  cfg.Address,
						Database: cfg.Database,
						Type:     log.ARDBPrimaryServer,
						VdiskID:  ds.vdiskID,
					},
				)
			}
		}
	}
	return err
}

// Close implements BlockStorage.Close
func (ds *dedupedStorage) Close() error { return nil }

// DedupedVdiskExists returns if the deduped vdisk in question
// exists in the given ardb storage cluster.
func DedupedVdiskExists(vdiskID string, cluster *config.StorageClusterConfig) (bool, error) {
	if cluster == nil {
		return false, errors.New("no cluster config given")
	}

	// storage key used on all data servers for this vdisk
	key := lba.StorageKey(vdiskID)

	// go through each server to check if the vdisKID exists there
	// the first vdisk which has data for this vdisk,
	// we'll take as a sign that the vdisk exists
	for _, serverConfig := range cluster.Servers {
		exists, err := dedupedVdiskExistsOnServer(key, serverConfig)
		if exists || err != nil {
			return exists, err
		}
	}

	// no errors occured, but no server had the given storage key,
	// which means the vdisk doesn't exist
	return false, nil
}

func dedupedVdiskExistsOnServer(key string, server config.StorageServerConfig) (bool, error) {
	conn, err := ardb.GetConnection(server)
	if err != nil {
		return false, fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
	}
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}

// ListDedupedBlockIndices returns all indices stored for the given deduped storage.
// This function will always either return an error OR indices.
// If this function returns indices, they are guaranteed to be in order from smallest to biggest.
func ListDedupedBlockIndices(vdiskID string, cluster *config.StorageClusterConfig) ([]int64, error) {
	if cluster == nil {
		return nil, errors.New("no cluster config given")
	}

	var vdiskIndices []int64
	key := lba.StorageKey(vdiskID)

	// collect all deduped block indices
	// from all the different data servers which make up the given storage cluster.
	var err error
	var serverIndices []int64
	for _, server := range cluster.Servers {
		serverIndices, err = listDedupedBlockIndices(key, server)
		if err != nil {
			if err == redis.ErrNil {
				continue
			}
			return nil, err
		}

		vdiskIndices = append(vdiskIndices, serverIndices...)
	}

	// if no vdisk indices are found on any of the given storage servers,
	// we'll return a redis nil-error, such that the user can decide
	// if they want to threat it as an error or not
	if len(vdiskIndices) == 0 {
		return nil, redis.ErrNil
	}

	// at least one index is found,
	// make sure it's sorted in ascending order and return them all.
	sortInt64s(vdiskIndices)
	return vdiskIndices, nil
}

func listDedupedBlockIndices(key string, server config.StorageServerConfig) ([]int64, error) {
	conn, err := ardb.GetConnection(server)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
	}
	defer conn.Close()
	return ardb.RedisInt64s(listDedupedBlockIndicesScript.Do(conn, key))
}

// CopyDeduped copies all metadata of a deduped storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func CopyDeduped(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// validate source cluster
	if sourceCluster == nil {
		return errors.New("no source cluster given")
	}

	// copy the source LBA to a target LBA within the same cluster
	if targetCluster == nil {
		return copyDedupedSameServerCount(sourceID, targetID, sourceCluster, sourceCluster)
	}

	// copy the source LBA to a target LBA,
	// between clusters with an equal server count
	if len(sourceCluster.Servers) == len(targetCluster.Servers) {
		return copyDedupedSameServerCount(sourceID, targetID, sourceCluster, targetCluster)
	}

	// copy the source LBA to a target LBA,
	// between clusters with a different server count
	return copyDedupedDifferentServerCount(sourceID, targetID, sourceCluster, targetCluster)
}

func copyDedupedSameServerCount(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	var err error
	var targetCfg config.StorageServerConfig

	for index, sourceCfg := range sourceCluster.Servers {
		targetCfg = targetCluster.Servers[index]

		if sourceCfg.Equal(&targetCfg) {
			// within same storage server
			err = func() error {
				conn, err := ardb.GetConnection(sourceCfg)
				if err != nil {
					return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
				}
				defer conn.Close()

				return copyDedupedSameConnection(sourceID, targetID, conn)
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

				return copyDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
			}()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func copyDedupedDifferentServerCount(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// create the target LBA sector storage,
	// which will be used to store the source LBA sectors into the target ARDB cluster.
	provider, err := ardb.StaticProviderFromStorageCluster(*targetCluster, nil)
	if err != nil {
		return err
	}
	targetStorage := lba.ARDBSectorStorage(targetID, provider)

	// copy all the source sectors into the target ARDB cluster,
	// one source ARDB server at a time.
	for _, sourceCfg := range sourceCluster.Servers {
		err = copyDedupedUsingSectorStorage(sourceID, targetID, sourceCfg, targetStorage)
		if err != nil {
			return err
		}
	}

	// all source sectors were succesfully copied.
	return nil
}

func copyDedupedSameConnection(sourceID, targetID string, conn redis.Conn) (err error) {
	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	sourceKey, targetKey := lba.StorageKey(sourceID), lba.StorageKey(targetID)
	indexCount, err := redis.Int64(copyDedupedSameConnScript.Do(conn, sourceKey, targetKey))
	if err == nil {
		log.Infof("copied %d meta indices to vdisk %q", indexCount, targetID)
	}

	return
}

func copyDedupedDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) (err error) {
	sourceKey, targetKey := lba.StorageKey(sourceID), lba.StorageKey(targetID)

	// get data from source connection
	log.Infof("collecting all metadata from source vdisk %q...", sourceID)
	data, err := redis.StringMap(connA.Do("HGETALL", sourceKey))
	if err != nil {
		return
	}
	dataLength := len(data)
	if dataLength == 0 {
		return // nothing to do
	}

	log.Infof("collected %d meta indices from source vdisk %q",
		dataLength, sourceID)

	// start the copy transaction
	if err = connB.Send("MULTI"); err != nil {
		return
	}

	// delete any existing vdisk
	if err = connB.Send("DEL", targetKey); err != nil {
		return
	}

	// buffer all data on target connection
	log.Infof("buffering %d meta indices for target vdisk %q...",
		len(data), targetID)
	var index int64
	for rawIndex, hash := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", targetKey, index, []byte(hash))
	}

	// send all data to target connection (execute the transaction)
	log.Infof("flushing buffered metadata for target vdisk %q...", targetID)
	response, err := connB.Do("EXEC")
	if err == nil && response == nil {
		// if response == <nil> the transaction has failed
		// more info: https://redis.io/topics/transactions
		err = fmt.Errorf("vdisk %q was busy and couldn't be modified", targetID)
	}

	return
}

func copyDedupedUsingSectorStorage(sourceID, targetID string, sourceCfg config.StorageServerConfig, storage lba.SectorStorage) error {
	conn, err := ardb.GetConnection(sourceCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	sourceKey := lba.StorageKey(sourceID)

	// get data from source connection
	log.Infof("collecting all metadata from source vdisk %q...", sourceID)
	sectors, err := ardb.RedisInt64ToBytesMapping(conn.Do("HGETALL", sourceKey))
	if err != nil {
		return err
	}

	sectorLength := len(sectors)
	if sectorLength == 0 {
		return nil // nothing to do
	}

	log.Infof("collected %d sectors from source vdisk %q, storing them now...", sectorLength, sourceID)

	var sector *lba.Sector
	for index, bytes := range sectors {
		sector, err = lba.SectorFromBytes(bytes)
		if err != nil {
			return fmt.Errorf("invalid raw sector bytes at sector index %d: %v", index, err)
		}

		err = storage.SetSector(index, sector)
		if err != nil {
			return fmt.Errorf("couldn't set target sector %d: %v", index, err)
		}
	}

	log.Infof("set %d target sectors from target vdisk %q, flushing now...", sectorLength, targetID)

	return storage.Flush()
}

func newDeleteDedupedMetadataOp(vdiskID string) storageOp {
	return &deleteDedupedMetadataOp{vdiskID}
}

type deleteDedupedMetadataOp struct {
	vdiskID string
}

func (op *deleteDedupedMetadataOp) Send(sender storageOpSender) error {
	log.Debugf("batch deletion of deduped metadata for: %v", op.vdiskID)
	return sender.Send("DEL", lba.StorageKey(op.vdiskID))
}

func (op *deleteDedupedMetadataOp) Receive(receiver storageOpReceiver) error {
	_, err := receiver.Receive()
	return err
}

func (op *deleteDedupedMetadataOp) Label() string {
	return "delete deduped metadata of " + op.vdiskID
}

var copyDedupedSameConnScript = redis.NewScript(0, `
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

var listDedupedBlockIndicesScript = redis.NewScript(0, fmt.Sprintf(`
local key = ARGV[1]
local sectors = redis.call("HGETALL", key)

local indices = {}
for rsi = 1, #sectors, 2 do
	local si = sectors[rsi]
	local s = sectors[rsi+1]

	-- go through each hash, to check if we need to add it
	for hi = 1, %[1]d do
		local hashStart = (hi - 1) * %[2]d + 1
		local hashEnd = hi * %[2]d
		for i = hashStart, hashEnd do
			if s:byte(i) ~= 0 then
				-- hash is non-nil, so let's save it
				local blockIndex = (si * %[1]d) + (hi - 1)
				indices[#indices+1] = blockIndex
				break
			end
		end
	end
end

-- return all found hashes
return indices
`, lba.NumberOfRecordsPerLBASector, zerodisk.HashSize))
