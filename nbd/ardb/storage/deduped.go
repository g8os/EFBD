package storage

import (
	"context"
	"fmt"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
)

// Deduped returns a deduped BlockStorage
func Deduped(cfg BlockStorageConfig, cluster, templateCluster ardb.StorageCluster) (BlockStorage, error) {
	// define the LBA cache limit
	cacheLimit := cfg.LBACacheLimit
	if cacheLimit < lba.BytesPerSector {
		log.Infof(
			"LBACacheLimit (%d) will be defaulted to %d (min-capped)",
			cacheLimit, lba.BytesPerSector)
		cacheLimit = ardb.DefaultLBACacheLimit
	}

	// create the LBA (used to store deduped metadata)
	lbaStorage := newLBASectorStorage(cfg.VdiskID, cluster)
	vlba, err := lba.NewLBA(cacheLimit, lbaStorage)
	if err != nil {
		log.Errorf("couldn't create the LBA: %s", err.Error())
		return nil, err
	}

	dedupedStorage := &dedupedStorage{
		zeroContentHash: zerodisk.HashBytes(make([]byte, cfg.BlockSize)),
		cluster:         cluster,
		lba:             vlba,
	}

	dedupedStorage.wpool = Pool{
		Work: dedupedStorage.commitBlock,
	}

	if err := dedupedStorage.wpool.Open(); err != nil {
		return nil, err
	}

	size := int(CacheSize / cfg.BlockSize)
	dedupedStorage.cache = NewCache(dedupedStorage.evictCache, 0, 0, size)

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
	cache           *Cache               // write cache
	wpool           Pool                 // writer pool
}

type writeRequest struct {
	hash    zerodisk.Hash
	content []byte
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
	ds.setContent(hash, content)

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
	ds.cache.Flush()
	err = ds.lba.Flush()
	return
}

// getPrimaryContent gets content from the primary storage.
// Assigned to (*dedupedStorage).getContent in case this storage has no template support.
func (ds *dedupedStorage) getPrimaryContent(hash zerodisk.Hash) ([]byte, error) {
	if content, ok := ds.cache.Get(hash); ok {
		return content, nil
	}

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
		if errors.Cause(err) == ErrClusterNotDefined {
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
	ds.setContent(hash, content)
	log.Debugf(
		"content not available in primary/slave storage for %v, but did find it in template storage",
		hash)

	// err = nil, content != nil
	return
}

func (ds *dedupedStorage) commitBlock(in interface{}) interface{} {
	request := in.(*writeRequest)
	hash := request.hash
	cmd := ardb.Command(command.Set, hash.Bytes(), request.content)
	if err := ardb.Error(ds.cluster.DoFor(int64(hash[0]), cmd)); err != nil {
		log.Errorf("couldn't store content in primary/slave storage: %s", err.Error())
	}

	return nil
}

func (ds *dedupedStorage) evictCache(hash zerodisk.Hash, content []byte) {
	if err := ds.wpool.Do(&writeRequest{hash: hash, content: content}, nil); err != nil {
		log.Errorf("couldn't store content in primary/slave storage: %s", err.Error())
	}
}

// setContent if it doesn't exist yet,
// and increase the reference counter, by adding this vdiskID
func (ds *dedupedStorage) setContent(hash zerodisk.Hash, content []byte) {
	ds.cache.Set(hash, content)
}

// Close implements BlockStorage.Close
func (ds *dedupedStorage) Close() error {
	ds.cache.Close()
	return ds.wpool.Close()
}

// dedupedVdiskExists checks if a deduped vdisks exists on a given cluster
func dedupedVdiskExists(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverCh, err := cluster.ServerIterator(ctx)
	if err != nil {
		return false, err
	}

	var exists bool
	action := ardb.Command(command.Exists, lbaStorageKey(vdiskID))
	for server := range serverCh {
		log.Infof("checking if deduped vdisk %s exists on %v", vdiskID, server.Config())
		exists, err = ardb.Bool(server.Do(action))
		if err != nil || exists {
			return exists, err
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
	action := ardb.Command(command.Delete, lbaStorageKey(vdiskID))
	for server := range serverCh {
		server := server
		go func() {
			log.Infof("deleting deduped data from vdisk %s on %v", vdiskID, server.Config())
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
	action := ardb.Script(0, listDedupedBlockIndicesScriptSource, nil, lbaStorageKey(vdiskID))
	for server := range serverCh {
		server := server
		go func() {
			var result serverResult
			log.Infof("listing block indices from deduped vdisk %s on %v", vdiskID, server.Config())
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

// copyDedupedMetadata copies all metadata of a deduped storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func copyDedupedMetadata(sourceID, targetID string, sourceBS, targetBS int64, sourceCluster, targetCluster ardb.StorageCluster) error {
	if sourceBS != targetBS {
		return errors.Newf(
			"vdisks %s and %s have non matching block sizes (%d != %d)",
			sourceID, targetID, sourceBS, targetBS)
	}

	if isInterfaceValueNil(targetCluster) {
		log.Infof(
			"copying deduped (LBA) metadata from vdisk %s to vdisk %s within a single storage cluster...",
			sourceID, targetID)
		return copyDedupedSameCluster(sourceID, targetID, sourceCluster)
	}

	if sourceCluster.ServerCount() == targetCluster.ServerCount() {
		log.Infof(
			"copying deduped (LBA) metadata from vdisk %s to vdisk %s between clusters wihh an equal amount of servers...",
			sourceID, targetID)
		return copyDedupedSameServerCount(sourceID, targetID, sourceCluster, targetCluster)
	}

	log.Infof(
		"copying deduped (LBA) metadata from vdisk %s to vdisk %s between clusters wihh an different amount of servers...",
		sourceID, targetID)
	return copyDedupedDifferentServerCount(sourceID, targetID, sourceCluster, targetCluster)
}

func copyDedupedSameCluster(sourceID, targetID string, cluster ardb.StorageCluster) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := cluster.ServerIterator(ctx)
	if err != nil {
		return err
	}

	sourceKey, targetKey := lbaStorageKey(sourceID), lbaStorageKey(targetID)
	action := ardb.Script(0, copyDedupedSameConnScriptSource,
		[]string{targetKey}, sourceKey, targetKey)

	type copyResult struct {
		Count int64
		Error error
	}
	resultChan := make(chan copyResult)

	var actionCount int
	for server := range ch {
		server := server
		go func() {
			cfg := server.Config()
			var result copyResult
			log.Debugf(
				"copying deduped (LBA) metadata from vdisk %s to vdisk %s on server %s",
				sourceID, targetID, &cfg)
			result.Count, result.Error = ardb.Int64(server.Do(action))
			log.Debugf("copied %d LBA sectors to vdisk %s on server %s",
				result.Count, targetID, &cfg)
			select {
			case resultChan <- result:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var totalCount int64
	var result copyResult
	for i := 0; i < actionCount; i++ {
		result = <-resultChan
		if result.Error != nil {
			log.Errorf(
				"stop of copying deduped (LBA) metadata from vdisk %s to vdisk %s due to an error: %v",
				sourceID, targetID, result.Error)
			return result.Error
		}
		totalCount += result.Count
	}

	if totalCount == 0 {
		return errors.Newf(
			"zero LBA sectors have been copied from vdisk %s to vdisk %s",
			sourceID, targetID)
	}

	return nil
}

func copyDedupedSameServerCount(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcChan, err := sourceCluster.ServerIterator(ctx)
	if err != nil {
		return err
	}
	dstChan, err := targetCluster.ServerIterator(ctx)
	if err != nil {
		return err
	}

	sourceKey := lbaStorageKey(sourceID)
	targetKey := lbaStorageKey(targetID)

	sameConnAction := ardb.Script(0, copyDedupedSameConnScriptSource,
		[]string{targetKey}, sourceKey, targetKey)

	type copyResult struct {
		Count int64
		Error error
	}
	resultChan := make(chan copyResult)

	var actionCount int
	// spawn all copy actions
	for {
		// get source and target server
		src, ok := <-srcChan
		if !ok {
			break
		}
		dst, ok := <-dstChan
		if !ok {
			panic("destination servers ran out before source servers")
		}

		go func() {
			var result copyResult
			srcConfig := src.Config()
			if srcConfig.Equal(dst.Config()) {
				log.Debugf(
					"copy deduped (LBA) metadata from vdisk %s to vdisk %s on server %s",
					sourceID, targetID, src.Config())
				result.Count, result.Error = ardb.Int64(src.Do(sameConnAction))
			} else {
				log.Debugf(
					"copy deduped (LBA) metadata from vdisk %s (at %s) to vdisk %s (at %s)",
					sourceID, src.Config(), targetID, dst.Config())
				result.Count, result.Error = copyDedupedBetweenServers(sourceKey, targetKey, src, dst)
			}

			select {
			case resultChan <- result:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var totalCount int64
	var result copyResult
	for i := 0; i < actionCount; i++ {
		result = <-resultChan
		if result.Error != nil {
			log.Errorf(
				"stop of copying deduped (LBA) metadata from vdisk %s to vdisk %s due to an error: %v",
				sourceID, targetID, result.Error)
			return result.Error
		}
		totalCount += result.Count
	}

	if totalCount == 0 {
		return errors.Newf(
			"zero LBA sectors have been copied from vdisk %s to vdisk %s",
			sourceID, targetID)
	}

	return nil
}

func copyDedupedDifferentServerCount(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) error {
	// copy all the source sectors into the target ARDB cluster,
	// one source ARDB server at a time.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcChan, err := sourceCluster.ServerIterator(ctx)
	if err != nil {
		return err
	}

	sourceKey := lbaStorageKey(sourceID)
	targetStorage := newLBASectorStorage(targetID, targetCluster)

	type copyResult struct {
		Count int64
		Error error
	}
	resultChan := make(chan copyResult)

	var actionCount int
	for server := range srcChan {
		server := server
		go func() {
			var result copyResult
			result.Count, result.Error = copyDedupedMetadataToLBAStorage(sourceKey, server, targetStorage)
			select {
			case resultChan <- result:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var totalCount int64
	var result copyResult
	for i := 0; i < actionCount; i++ {
		result = <-resultChan
		if result.Error != nil {
			return result.Error
		}
		totalCount += result.Count
	}

	if totalCount == 0 {
		return errors.Newf(
			"zero LBA sectors have been copied from vdisk %s to vdisk %s",
			sourceID, targetID)
	}

	return nil
}

type dedupFetchResult struct {
	Data  map[int64][]byte
	Error error
}

func dedupMetadataFetcher(ctx context.Context, storageKey string, server ardb.StorageServer) <-chan dedupFetchResult {
	const (
		startCursor = "0"
		itemCount   = "1000"
	)

	ch := make(chan dedupFetchResult)
	go func() {
		defer close(ch)

		serverCfg := server.Config()
		// get data from source connection
		log.Debugf("collecting all (LBA) metadata from %s on %s...", storageKey, &serverCfg)

		var slice interface{}
		var result dedupFetchResult

		// initial cursor and action
		cursor := startCursor
		action := ardb.Command(command.HashScan, storageKey, cursor, "COUNT", itemCount)

		// loop through all values of the mapping
		for {
			// get new cursor and raw data
			cursor, slice, result.Error = ardb.CursorAndValues(server.Do(action))
			if result.Error == nil {
				// if succesfull, convert the raw data to a mapping we can use
				result.Data, result.Error = ardb.Int64ToBytesMapping(slice, nil)
				log.Debugf("received %d LBA sectors from %s on %s...",
					len(result.Data), storageKey, &serverCfg)
			}

			select {
			case ch <- result:
			case <-ctx.Done():
				return
			}

			// return in case of an error or when we iterated through all possible values
			if result.Error != nil || cursor == startCursor {
				return
			}

			// continue going, prepare the action for the next iteration
			action = ardb.Command(command.HashScan, storageKey, cursor, "COUNT", itemCount)
		}
	}()
	return ch
}

func copyDedupedBetweenServers(sourceKey, targetKey string, src, dst ardb.StorageServer) (int64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type copyResult struct {
		Count int64
		Error error
	}
	resultChan := make(chan copyResult)

	var actionCount int
	ch := dedupMetadataFetcher(ctx, sourceKey, src)
	for input := range ch {
		input := input
		go func() {
			result := copyResult{
				Count: int64(len(input.Data)),
				Error: input.Error,
			}
			if result.Error != nil || result.Count == 0 {
				select {
				case resultChan <- result:
				case <-ctx.Done():
				}
				return
			}

			log.Debugf("collected %d sector indices from %s...", result.Count, sourceKey)

			var cmds []ardb.StorageAction

			// buffer all set actions
			log.Debugf("buffering %d sector indices to be stored at %s...", result.Count, targetKey)
			for index, hash := range input.Data {
				cmds = append(cmds,
					ardb.Command(command.HashSet, targetKey, index, hash))
			}

			action := ardb.Commands(cmds...)
			log.Debugf("flushing buffered metadata to be stored at %s on %s...", targetKey, dst.Config())
			result.Error = ardb.Error(dst.Do(action))

			select {
			case resultChan <- result:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var totalCount int64
	var result copyResult
	for i := 0; i < actionCount; i++ {
		result = <-resultChan
		if result.Error != nil {
			log.Errorf("stop copying sectors from %s to %s due to an error: %v",
				sourceKey, targetKey, result.Error)
			return 0, result.Error
		}
		totalCount += result.Count
	}

	return totalCount, nil
}

func copyDedupedMetadataToLBAStorage(sourceKey string, src ardb.StorageServer, storage lba.SectorStorage) (int64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type copyResult struct {
		Count int64
		Error error
	}
	resultChan := make(chan copyResult)

	var actionCount int
	ch := dedupMetadataFetcher(ctx, sourceKey, src)
	for input := range ch {
		input := input
		go func() {
			result := copyResult{
				Count: int64(len(input.Data)),
				Error: input.Error,
			}
			if result.Error != nil || result.Count == 0 {
				select {
				case resultChan <- result:
				case <-ctx.Done():
				}
				return
			}

			log.Debugf("collected %d sectors from %s...", result.Count, sourceKey)

			var err error
			var sector *lba.Sector

			// NOTE:
			// for now this is a bit slow,
			// as we'll reach out to the target server for each iterator
			for index, bytes := range input.Data {
				sector, err = lba.SectorFromBytes(bytes)
				if err != nil {
					err = errors.Wrapf(err, "invalid raw sector bytes at sector index %d", index)
					break
				}

				err = storage.SetSector(index, sector)
				if err != nil {
					err = errors.Wrapf(err, "couldn't set sector %d", index)
					break
				}
			}
			if err == nil {
				log.Debugf("stored %d LBA sectors for %s (using Slow LBA Storage Respreading)...",
					result.Count, sourceKey)
			}

			result.Error = err
			select {
			case resultChan <- result:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var totalCount int64
	var result copyResult
	for i := 0; i < actionCount; i++ {
		result = <-resultChan
		if result.Error != nil {
			return 0, result.Error
		}
		totalCount += result.Count
	}

	return totalCount, nil
}

const copyDedupedSameConnScriptSource = `
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
`

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
