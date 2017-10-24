package storage

import (
	"context"
	"fmt"

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
			log.Infof("checking if non-deduped vdisk %s exists on %v",
				vdiskID, server.Config())
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
			log.Infof("deleting blocks from non-deduped vdisk %s on %v",
				vdiskID, server.Config())
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
			log.Infof("listing block indices from non-deduped vdisk %s on %v",
				vdiskID, server.Config())
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

// copyNonDedupedData copies all data of a non-deduped storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func copyNonDedupedData(sourceID, targetID string, sourceBS, targetBS int64, sourceCluster, targetCluster ardb.StorageCluster) error {
	if sourceBS != targetBS {
		return fmt.Errorf(
			"vdisks %s and %s have non matching block sizes (%d != %d)",
			sourceID, targetID, sourceBS, targetBS)
	}

	if isInterfaceValueNil(targetCluster) {
		log.Infof(
			"copying non-deduped data from vdisk %s to vdisk %s within a single storage cluster...",
			sourceID, targetID)
		return copyNonDedupedSameCluster(sourceID, targetID, sourceCluster)
	}

	if sourceCluster.ServerCount() == targetCluster.ServerCount() {
		log.Infof(
			"copying non-deduped data from vdisk %s to vdisk %s between clusters wihh an equal amount of servers...",
			sourceID, targetID)
		return copyNonDedupedSameServerCount(sourceID, targetID, sourceCluster, targetCluster)
	}

	log.Infof(
		"copying non-deduped data from vdisk %s to vdisk %s between clusters wihh an different amount of servers...",
		sourceID, targetID)
	return copyNonDedupedDifferentServerCount(sourceID, targetID, targetBS, sourceCluster, targetCluster)
}

func copyNonDedupedSameCluster(sourceID, targetID string, cluster ardb.StorageCluster) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := cluster.ServerIterator(ctx)
	if err != nil {
		return err
	}

	action := ardb.Script(0, copyNonDedupedSameConnScriptSource,
		[]string{nonDedupedStorageKey(targetID)}, sourceID, targetID)

	resultChan := make(chan error)

	var actionCount int
	for server := range ch {
		server := server
		go func() {
			log.Debugf(
				"copying non-deduped data from vdisk %s to vdisk %s on server %s",
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
				"stop of copying non-deduped data from vdisk %s to vdisk %s due to an error: %v",
				sourceID, targetID, err)
			return err
		}
	}

	return nil
}

func copyNonDedupedSameServerCount(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) error {
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

	sourceKey := nonDedupedStorageKey(sourceID)
	targetKey := nonDedupedStorageKey(targetID)

	sameConnAction := ardb.Script(0, copyNonDedupedSameConnScriptSource,
		[]string{targetKey}, sourceID, targetID)

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
					"copy non-deduped data from vdisk %s to vdisk %s on server %s",
					sourceID, targetID, src.Config())
				err = ardb.Error(src.Do(sameConnAction))
			} else {
				log.Debugf(
					"copy non-deduped data from vdisk %s (at %s) to vdisk %s (at %s)",
					sourceID, src.Config(), targetID, dst.Config())
				err = copyNonDedupedBetweenServers(sourceKey, targetKey, src, dst)
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
				"stop of copying non-deduped data from vdisk %s to vdisk %s due to an error: %v",
				sourceID, targetID, err)
			return err
		}
	}

	return nil
}

func copyNonDedupedDifferentServerCount(sourceID, targetID string, targetBS int64, sourceCluster, targetCluster ardb.StorageCluster) error {
	// copy all the source sectors into the target ARDB cluster,
	// one source ARDB server at a time.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcChan, err := sourceCluster.ServerIterator(ctx)
	if err != nil {
		return err
	}

	targetStorage, err := NonDeduped(targetID, "", targetBS, targetCluster, nil)
	if err != nil {
		return err
	}

	sourceKey := nonDedupedStorageKey(sourceID)
	resultChan := make(chan error)

	var actionCount int
	for server := range srcChan {
		server := server
		go func() {
			err := copyNonDedupDataToBlockStorage(sourceKey, server, targetStorage)
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

type nonDedupFetchResult struct {
	Data  map[int64][]byte
	Error error
}

func nonDedupDataFetcher(ctx context.Context, storageKey string, server ardb.StorageServer) <-chan nonDedupFetchResult {
	ch := make(chan nonDedupFetchResult)
	go func() {
		defer close(ch)

		// get data from source connection
		log.Debugf("collecting all nondedup blocks from %s on %s...", storageKey, server.Config())

		// TODO: replace this with a cursor-based approach,
		// so we don't have too much in memory at once
		// issue: https://github.com/zero-os/0-Disk/issues/353
		action := ardb.Command(command.HashGetAll, storageKey)

		var result nonDedupFetchResult
		result.Data, result.Error = ardb.Int64ToBytesMapping(server.Do(action))

		select {
		case ch <- result:
		case <-ctx.Done():
		}
	}()
	return ch
}

func copyNonDedupedBetweenServers(sourceKey, targetKey string, src, dst ardb.StorageServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := make(chan error)

	var actionCount int
	ch := nonDedupDataFetcher(ctx, sourceKey, src)
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

			log.Debugf("collected %d nondedup blocks from %s (at %s)...",
				dataLength, sourceKey, src.Config())

			cmds := []ardb.StorageAction{
				// delete any existing vdisk
				ardb.Command(command.Delete, targetKey),
			}

			// buffer all set actions
			log.Debugf("buffering %d nondedup blocks to be stored at %s...", dataLength, targetKey)
			for index, hash := range input.Data {
				cmds = append(cmds,
					ardb.Command(command.HashSet, targetKey, index, hash))
			}

			transaction := ardb.Transaction(cmds...)
			log.Debugf("flushing buffered data to be stored at %s on %s...", targetKey, dst.Config())
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
			return
		}()
		actionCount++
	}

	// collect all results
	var err error
	for i := 0; i < actionCount; i++ {
		err = <-resultChan
		if err != nil {
			log.Errorf("stop copying nondedup blocks from %s to %s due to an error: %v",
				sourceKey, targetKey, err)
			return err
		}
	}

	return nil
}

func copyNonDedupDataToBlockStorage(sourceKey string, src ardb.StorageServer, storage BlockStorage) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := make(chan error)

	var actionCount int
	ch := nonDedupDataFetcher(ctx, sourceKey, src)
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

			log.Debugf("collected %d nondedup blocks from %s (at %s)...",
				dataLength, sourceKey, src.Config())

			var err error

			// NOTE:
			// for now this is a bit slow,
			// as we'll reach out to the target server for each iterator
			for index, bytes := range input.Data {
				err = storage.SetBlock(index, bytes)
				if err != nil {
					err = fmt.Errorf("couldn't set block %d: %v", index, err)
					break
				}
			}

			if err == nil {
				log.Debugf("flushing %d nondedup stored blocks from %s (at %s)...",
					dataLength, sourceKey, src.Config())
				err = storage.Flush()
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

// nonDedupedStorageKey returns the storage key that can/will be
// used to store the nondeduped data for the given vdiskID
func nonDedupedStorageKey(vdiskID string) string {
	return nonDedupedStorageKeyPrefix + vdiskID
}

const (
	// nonDedupedStorageKeyPrefix is the prefix used in nonDedupedStorageKey
	nonDedupedStorageKeyPrefix = "nondedup:"
)

const copyNonDedupedSameConnScriptSource = `
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
