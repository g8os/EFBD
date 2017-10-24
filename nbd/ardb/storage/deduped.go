package storage

import (
	"context"
	"fmt"

	"github.com/zero-os/0-Disk"
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
			log.Infof("checking if deduped vdisk %s exists on %v", vdiskID, server.Config())
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
	action := ardb.Script(0, listDedupedBlockIndicesScriptSource, nil, lba.StorageKey(vdiskID))
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
		return fmt.Errorf(
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

	action := ardb.Script(0, copyDedupedSameConnScriptSource,
		[]string{lba.StorageKey(targetID)}, sourceID, targetID)

	resultChan := make(chan error)

	var actionCount int
	for server := range ch {
		server := server
		go func() {
			log.Debugf(
				"copying deduped (LBA) metadata from vdisk %s to vdisk %s on server %s",
				sourceID, targetID, server.Config())
			err = ardb.Error(server.Do(action))
			select {
			case resultChan <- err:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	for i := 0; i < actionCount; i++ {
		err = <-resultChan
		if err != nil {
			log.Errorf(
				"stop of copying deduped (LBA) metadata from vdisk %s to vdisk %s due to an error: %v",
				sourceID, targetID, err)
			return err
		}
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

	sourceKey := lba.StorageKey(sourceID)
	targetKey := lba.StorageKey(targetID)

	sameConnAction := ardb.Script(0, copyDedupedSameConnScriptSource,
		[]string{lba.StorageKey(targetID)}, sourceID, targetID)

	resultChan := make(chan error)
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
			var err error
			srcConfig := src.Config()
			if srcConfig.Equal(dst.Config()) {
				log.Debugf(
					"copy deduped (LBA) metadata from vdisk %s to vdisk %s on server %s",
					sourceID, targetID, src.Config())
				err = ardb.Error(src.Do(sameConnAction))
			} else {
				log.Debugf(
					"copy deduped (LBA) metadata from vdisk %s (at %s) to vdisk %s (at %s)",
					sourceID, src.Config(), targetID, dst.Config())
				err = copyDedupedBetweenServers(sourceKey, targetKey, src, dst)
			}

			select {
			case resultChan <- err:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	for i := 0; i < actionCount; i++ {
		err = <-resultChan
		if err != nil {
			log.Errorf(
				"stop of copying deduped (LBA) metadata from vdisk %s to vdisk %s due to an error: %v",
				sourceID, targetID, err)
			return err
		}
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

	sourceKey := lba.StorageKey(sourceID)
	targetStorage := lba.ARDBSectorStorage(targetID, targetCluster)
	resultChan := make(chan error)

	var actionCount int
	for server := range srcChan {
		server := server
		go func() {
			err := copyDedupedMetadataToLBAStorage(sourceKey, server, targetStorage)
			select {
			case resultChan <- err:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	for i := 0; i < actionCount; i++ {
		err = <-resultChan
		if err != nil {
			return err
		}
	}

	return nil
}

type dedupFetchResult struct {
	Data  map[int64][]byte
	Error error
}

func dedupMetadataFetcher(ctx context.Context, storageKey string, server ardb.StorageServer) <-chan dedupFetchResult {
	ch := make(chan dedupFetchResult)
	go func() {
		defer close(ch)

		// get data from source connection
		log.Debugf("collecting all (LBA) metadata from %s on %s...", storageKey, server.Config())

		// TODO: replace this with a cursor-based approach,
		// so we don't have too much in memory at once
		// issue: https://github.com/zero-os/0-Disk/issues/353
		action := ardb.Command(command.HashGetAll, storageKey)

		var result dedupFetchResult
		result.Data, result.Error = ardb.Int64ToBytesMapping(server.Do(action))

		select {
		case ch <- result:
		case <-ctx.Done():
		}
	}()
	return ch
}

func copyDedupedBetweenServers(sourceKey, targetKey string, src, dst ardb.StorageServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := make(chan error)

	var actionCount int
	ch := dedupMetadataFetcher(ctx, sourceKey, src)
	for input := range ch {
		input := input
		go func() {
			dataLength := len(input.Data)
			if input.Error != nil || dataLength == 0 {
				select {
				case resultChan <- input.Error:
				case <-ctx.Done():
				}
				return
			}

			log.Debugf("collected %d sector indices from %s...", dataLength, sourceKey)

			cmds := []ardb.StorageAction{
				// delete any existing vdisk
				ardb.Command(command.Delete, targetKey),
			}

			// buffer all set actions
			log.Debugf("buffering %d sector indices to be stored at %s...", dataLength, targetKey)
			for index, hash := range input.Data {
				cmds = append(cmds,
					ardb.Command(command.HashSet, targetKey, index, hash))
			}

			transaction := ardb.Transaction(cmds...)
			log.Debugf("flushing buffered metadata to be stored at %s on %s...", targetKey, dst.Config())
			// execute the transaction
			response, err := dst.Do(transaction)
			if err == nil && response == nil {
				// if response == <nil> the transaction has failed
				// more info: https://redis.io/topics/transactions
				err = fmt.Errorf("%s was busy and couldn't be modified", targetKey)
			}

			select {
			case resultChan <- err:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var err error
	for i := 0; i < actionCount; i++ {
		err = <-resultChan
		if err != nil {
			log.Errorf("stop copying sectors from %s to %s due to an error: %v",
				sourceKey, targetKey, err)
			return err
		}
	}

	return nil
}

func copyDedupedMetadataToLBAStorage(sourceKey string, src ardb.StorageServer, storage lba.SectorStorage) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := make(chan error)

	var actionCount int
	ch := dedupMetadataFetcher(ctx, sourceKey, src)
	for input := range ch {
		input := input
		go func() {
			dataLength := len(input.Data)
			if input.Error != nil || dataLength == 0 {
				select {
				case resultChan <- input.Error:
				case <-ctx.Done():
				}
				return
			}

			log.Debugf("collected %d sectors from %s...", dataLength, sourceKey)

			var err error
			var sector *lba.Sector

			// NOTE:
			// for now this is a bit slow,
			// as we'll reach out to the target server for each iterator
			for index, bytes := range input.Data {
				sector, err = lba.SectorFromBytes(bytes)
				if err != nil {
					err = fmt.Errorf("invalid raw sector bytes at sector index %d: %v", index, err)
					break
				}

				err = storage.SetSector(index, sector)
				if err != nil {
					err = fmt.Errorf("couldn't set sector %d: %v", index, err)
					break
				}
			}

			if err == nil {
				log.Debugf("stored %d LBA sectors from %s (at %s)...",
					dataLength, sourceKey, src.Config())
			}

			select {
			case resultChan <- err:
			case <-ctx.Done():
			}
		}()
		actionCount++
	}

	// collect all results
	var err error
	for i := 0; i < actionCount; i++ {
		err = <-resultChan
		if err != nil {
			return err
		}
	}

	return nil
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
