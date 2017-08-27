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

func (ds *dedupedStorage) getDataConnection(hash zerodisk.Hash) (redis.Conn, error) {
	return ds.provider.DataConnection(int64(hash[0]))
}

func (ds *dedupedStorage) getTemplateConnection(hash zerodisk.Hash) (redis.Conn, error) {
	return ds.provider.TemplateConnection(int64(hash[0]))
}

// getPrimaryContent gets content from the primary storage.
// Assigned to (*dedupedStorage).getContent in case this storage has no template support.
func (ds *dedupedStorage) getPrimaryContent(hash zerodisk.Hash) (content []byte, err error) {
	conn, err := ds.getDataConnection(hash)
	if err != nil {
		return
	}
	defer conn.Close()

	content, err = ardb.RedisBytes(conn.Do("GET", hash.Bytes()))
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
	content = func() (content []byte) {
		conn, err := ds.getTemplateConnection(hash)
		if err != nil {
			log.Debugf(
				"content not available in primary storage for %v and no template storage available: %s",
				hash, err.Error())
			return
		}
		defer conn.Close()

		content, err = ardb.RedisBytes(conn.Do("GET", hash.Bytes()))
		if err != nil {
			content = nil
			log.Debugf(
				"content for %v not available in primary-, nor in template storage: %s",
				hash, err.Error())
		}

		return
	}()

	if content != nil {
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
	}

	// err = nil, content = ?
	return
}

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(hash zerodisk.Hash, content []byte) error {
	conn, err := ds.getDataConnection(hash)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", hash.Bytes(), content)
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
	if cluster.MetadataStorage == nil {
		return false, errors.New("no metadataServer given for cluster config")
	}

	conn, err := ardb.GetConnection(*cluster.MetadataStorage)
	if err != nil {
		return false, fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", lba.StorageKey(vdiskID)))
}

// ListDedupedBlockIndices returns all indices stored for the given deduped storage.
// This function will always either return an error OR indices.
// If this function returns indices, they are guaranteed to be in order from smallest to biggest.
func ListDedupedBlockIndices(vdiskID string, cluster *config.StorageClusterConfig) ([]int64, error) {
	if cluster == nil {
		return nil, errors.New("no cluster config given")
	}
	if cluster.MetadataStorage == nil {
		return nil, errors.New("no metadataServer given for cluster config")
	}

	conn, err := ardb.GetConnection(*cluster.MetadataStorage)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer conn.Close()
	indices, err := ardb.RedisInt64s(listDedupedBlockIndicesScript.Do(conn, lba.StorageKey(vdiskID)))
	if err != nil {
		return nil, err
	}

	sortInt64s(indices)
	return indices, nil
}

// CopyDeduped copies all metadata of a deduped storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func CopyDeduped(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// validate source cluster
	if sourceCluster == nil {
		return errors.New("no source cluster given")
	}
	if sourceCluster.MetadataStorage == nil {
		return errors.New("no metadataServer given for source")
	}

	// define whether or not we're copying between different servers,
	// and if the target cluster is given, make sure to validate it.
	var sameServer bool
	if targetCluster == nil {
		sameServer = true
		if sourceID == targetID {
			return errors.New(
				"sourceID and targetID can't be equal when copying within the same cluster")
		}
	} else {
		if targetCluster.MetadataStorage == nil {
			return errors.New("no metaDataServer given for target")
		}

		// even if targetCluster is given,
		// we could still be dealing with a duplicated cluster
		sameServer = *sourceCluster.MetadataStorage == *targetCluster.MetadataStorage
	}

	// within same storage server
	if sameServer {
		conn, err := ardb.GetConnection(*sourceCluster.MetadataStorage)
		if err != nil {
			return fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
		}
		defer conn.Close()

		return copyDedupedSameConnection(sourceID, targetID, conn)
	}

	// between different storage servers
	conns, err := ardb.GetConnections(
		*sourceCluster.MetadataStorage, *targetCluster.MetadataStorage)
	if err != nil {
		return fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
	}
	defer func() {
		conns[0].Close()
		conns[1].Close()
	}()

	return copyDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
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
		err = fmt.Errorf("%q does not exist", sourceID)
		return
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
