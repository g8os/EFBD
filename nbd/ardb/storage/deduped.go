package storage

import (
	"context"
	"fmt"

	"github.com/garyburd/redigo/redis"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
)

// Deduped returns a deduped BlockStorage
func Deduped(vdiskID string, blockSize, lbaCacheLimit int64, cluster, templateCluster ardb.StorageCluster) (BlockStorage, error) {
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
		cluster,
	)
	if err != nil {
		log.Errorf("couldn't create the LBA: %s", err.Error())
		return nil, err
	}

	dedupedStorage := &dedupedStorage{
		blockSize:       blockSize,
		vdiskID:         vdiskID,
		zeroContentHash: zerodisk.HashBytes(make([]byte, blockSize)),
		cluster:         cluster,
		lba:             vlba,
	}

	// getContent is ALWAYS defined,
	// but the actual function used depends on
	// whether or not this storage has template support.
	if isInterfaceValueNil(templateCluster) {
		dedupedStorage.getContent = dedupedStorage.getPrimaryContent
	} else {
		dedupedStorage.getContent = dedupedStorage.getPrimaryOrTemplateContent
		dedupedStorage.templateCluster = templateCluster
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
	cluster         ardb.StorageCluster  // used to interact with the ARDB (StorageEngine) Cluster
	templateCluster ardb.StorageCluster  // used to interact with the ARDB (StorageEngine) Template Cluster
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

// getPrimaryContent gets content from the primary storage.
// Assigned to (*dedupedStorage).getContent in case this storage has no template support.
func (ds *dedupedStorage) getPrimaryContent(hash zerodisk.Hash) (content []byte, err error) {
	cmd := ardb.Command(command.Get, hash.Bytes())
	return ardb.OptBytes(ds.cluster.DoFor(int64(hash[0]), cmd))
}

// getPrimaryOrTemplateContent gets content from the primary storage,
// or if the content can't be found in primary storage,
// we'll try to fetch it from the template storage.
// if the content is available in the template storage,
// we'll also try to store it in the primary storage before returning that content.
// Assigned to (*dedupedStorage).getContent in case this storage has template support.
func (ds *dedupedStorage) getPrimaryOrTemplateContent(hash zerodisk.Hash) (content []byte, err error) {
	// try to fetch it from the primary/slave storage
	content, err = ds.getPrimaryContent(hash)
	if err != nil || content != nil {
		return // critical err, or content is found
	}

	// try to fetch it from the template storage
	cmd := ardb.Command(command.Get, hash.Bytes())
	content, err = ardb.OptBytes(ds.templateCluster.DoFor(int64(hash[0]), cmd))
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

	// store template content in primary/slave storage asynchronously
	go func() {
		err := ds.setContent(hash, content)
		if err != nil {
			// we won't return error however, but just log it
			log.Errorf("couldn't store template content in primary/slave storage: %s", err.Error())
			return
		}

		log.Debugf(
			"stored template content for %v in primary/slave storage (asynchronously)",
			hash)
	}()

	log.Debugf(
		"content not available in primary/slave storage for %v, but did find it in template storage",
		hash)

	// err = nil, content != nil
	return
}

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(hash zerodisk.Hash, content []byte) error {
	cmd := ardb.Command(command.Set, hash.Bytes(), content)
	return ardb.Error(ds.cluster.DoFor(int64(hash[0]), cmd))
}

// Close implements BlockStorage.Close
func (ds *dedupedStorage) Close() error { return nil }

// dedupedVdiskExists checks if a deduped vdisks exists on a given cluster
func dedupedVdiskExists(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
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
	action := ardb.Command(command.Exists, lba.StorageKey(vdiskID))
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

// deleteDedupedData deletes the deduped data of a given vdisk from a given cluster.
func deleteDedupedData(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
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
	// TODO: dereference deduped blocks as well
	// https://github.com/zero-os/0-Disk/issues/88
	action := ardb.Command(command.Delete, lba.StorageKey(vdiskID))
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

// listDedupedBlockIndices lists all the block indices (sorted)
// from a deduped vdisk stored on a given cluster.
func listDedupedBlockIndices(vdiskID string, cluster ardb.StorageCluster) ([]int64, error) {
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
	action := ardb.Script(0, listDedupedBlockIndicesScriptSource, nil, lba.StorageKey(vdiskID))
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

// CopyDeduped copies all metadata of a deduped storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func CopyDeduped(sourceID, targetID string, sourceCluster config.StorageClusterConfig, targetCluster *config.StorageClusterConfig) error {
	// copy the source LBA to a target LBA within the same cluster
	if targetCluster == nil {
		return copyDedupedSameServerCount(sourceID, targetID, sourceCluster, sourceCluster)
	}

	// copy the source LBA to a target LBA,
	// between clusters with an equal server count
	if len(sourceCluster.Servers) == len(targetCluster.Servers) {
		return copyDedupedSameServerCount(sourceID, targetID, sourceCluster, *targetCluster)
	}

	// copy the source LBA to a target LBA,
	// between clusters with a different server count
	return copyDedupedDifferentServerCount(sourceID, targetID, sourceCluster, *targetCluster)
}

func copyDedupedSameServerCount(sourceID, targetID string, sourceCluster, targetCluster config.StorageClusterConfig) error {
	var err error
	var targetCfg config.StorageServerConfig

	for index, sourceCfg := range sourceCluster.Servers {
		targetCfg = targetCluster.Servers[index]

		if sourceCfg.Equal(targetCfg) {
			// within same storage server
			err = func() error {
				conn, err := ardb.Dial(sourceCfg)
				if err != nil {
					return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
				}
				defer conn.Close()

				return copyDedupedSameConnection(sourceID, targetID, conn)
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

				return copyDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
			}()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func copyDedupedDifferentServerCount(sourceID, targetID string, sourceCluster, targetCluster config.StorageClusterConfig) error {
	// create the target LBA sector storage,
	// which will be used to store the source LBA sectors into the target ARDB cluster.
	targetStorageCluster, err := ardb.NewCluster(targetCluster, nil)
	if err != nil {
		return err
	}
	targetStorage := lba.ARDBSectorStorage(targetID, targetStorageCluster)

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

func copyDedupedSameConnection(sourceID, targetID string, conn ardb.Conn) (err error) {
	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	sourceKey, targetKey := lba.StorageKey(sourceID), lba.StorageKey(targetID)
	indexCount, err := ardb.Int64(copyDedupedSameConnScript.Do(conn, sourceKey, targetKey))
	if err == nil {
		log.Infof("copied %d meta indices to vdisk %q", indexCount, targetID)
	}

	return
}

func copyDedupedDifferentConnections(sourceID, targetID string, connA, connB ardb.Conn) (err error) {
	sourceKey, targetKey := lba.StorageKey(sourceID), lba.StorageKey(targetID)

	// get data from source connection
	log.Infof("collecting all metadata from source vdisk %q...", sourceID)
	data, err := ardb.Int64ToBytesMapping(connA.Do("HGETALL", sourceKey))
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
	for index, hash := range data {
		connB.Send("HSET", targetKey, index, hash)
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
	conn, err := ardb.Dial(sourceCfg)
	if err != nil {
		return err
	}
	defer conn.Close()

	sourceKey := lba.StorageKey(sourceID)

	// get data from source connection
	log.Infof("collecting all metadata from source vdisk %q...", sourceID)
	sectors, err := ardb.Int64ToBytesMapping(conn.Do("HGETALL", sourceKey))
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
			return fmt.Errorf("couldn't set sector %d: %v", index, err)
		}
	}

	return nil
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

var listDedupedBlockIndicesScriptSource = fmt.Sprintf(`
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
`, lba.NumberOfRecordsPerLBASector, zerodisk.HashSize)
