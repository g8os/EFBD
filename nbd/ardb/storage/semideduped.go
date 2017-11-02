package storage

import (
	"context"

	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// SemiDeduped returns a semi deduped BlockStorage
func SemiDeduped(vdiskID string, blockSize, lbaCacheLimit int64, cluster, templateCluster ardb.StorageCluster) (BlockStorage, error) {
	templateStorage, err := Deduped(vdiskID, blockSize, lbaCacheLimit, cluster, templateCluster)
	if err != nil {
		return nil, err
	}

	userStorage, err := NonDeduped(vdiskID, "", blockSize, cluster, nil)
	if err != nil {
		templateStorage.Close()
		return nil, err
	}

	storage := &semiDedupedStorage{
		templateStorage: templateStorage,
		userStorage:     userStorage,
		vdiskID:         vdiskID,
		blockSize:       blockSize,
		cluster:         cluster,
	}

	err = storage.readBitMap()
	if err != nil {
		log.Debugf("couldn't read semi deduped storage %s's bitmap: %v", vdiskID, err)
	}

	return storage, nil
}

// semiDedupedStorage is a BlockStorage implementation,
// that stores the template content in the primary deduped storage,
// while it stores all user-written (and thus specific) data
// in the a nondeduped storage, both storages using the same storage servers.
type semiDedupedStorage struct {
	// used to store template data
	// (effectively read-only storage, from a user-perspective)
	templateStorage BlockStorage
	// used to store user-specific data
	// e.g. Modified Registers, Applications, ...
	userStorage BlockStorage

	// used to store the semi deduped metadata
	cluster ardb.StorageCluster

	// bitmap used to indicate if the data is available as userdata or not
	userStorageBitMap bitMap

	// ID of this storage's vdisk
	vdiskID string

	// used when merging content
	blockSize int64
}

// SetBlock implements BlockStorage.SetBlock
func (sds *semiDedupedStorage) SetBlock(blockIndex int64, content []byte) error {
	err := sds.userStorage.SetBlock(blockIndex, content)
	if err != nil {
		return err
	}

	// mark the bit in the bitmap,
	// such that the next time we retreive this block,
	// we know it has to be retreived from the user storage
	//
	// NOTE: for now this bit is never unset,
	// as it is assumed that once data is overwritten (if it's overwritten at all),
	// it is custom forever. It would be weird if suddenly out of the blue (after a delete operation for example),
	// the template (original) data would be used once again,
	// I don't think that's something a user would expect at all.
	sds.userStorageBitMap.Set(int(blockIndex))

	// delete content from dedup storage, as it's no longer needed there
	err = sds.templateStorage.DeleteBlock(blockIndex)
	if err != nil {
		// This won't be returned as an error,
		// as it's nothing critical,
		// it only means that deprecated data is not deleted.
		// It's not a critical error because the toggled bit in the bitmask,
		// will only make it look in the userstorage for this block anyhow.
		log.Error("semiDedupedStorage couldn't delete deprecated template data: ", err)
	}

	return nil
}

// GetBlock implements BlockStorage.GetBlock
func (sds *semiDedupedStorage) GetBlock(blockIndex int64) ([]byte, error) {
	// if a bit is enabled in the bitmap,
	// it means the data is stored in the user storage
	if sds.userStorageBitMap.Test(int(blockIndex)) {
		return sds.userStorage.GetBlock(blockIndex)
	}

	return sds.templateStorage.GetBlock(blockIndex)
}

// DeleteBlock implements BlockStorage.DeleteBlock
func (sds *semiDedupedStorage) DeleteBlock(blockIndex int64) error {
	errs := errors.NewErrorSlice()

	errs.Add(sds.templateStorage.DeleteBlock(blockIndex))

	// note that we don't unset the storage bit from the bitmask,
	// as that would basically flip it back to use dedup storage for this index,
	// which is not something we want,
	// as from a user perspective that already has been overwritten
	errs.Add(sds.userStorage.DeleteBlock(blockIndex))

	return errs.AsError()
}

// Flush implements BlockStorage.Flush
func (sds *semiDedupedStorage) Flush() error {
	errs := errors.NewErrorSlice()

	errs.Add(sds.templateStorage.Flush())
	errs.Add(sds.userStorage.Flush())

	// serialize bitmap
	errs.Add(sds.writeBitMap())

	return errs.AsError()
}

// Close implements BlockStorage.Close
func (sds *semiDedupedStorage) Close() error {
	errs := errors.NewErrorSlice()

	errs.Add(sds.templateStorage.Close())
	errs.Add(sds.userStorage.Close())
	return errs.AsError()
}

// readBitMap reads and decompresses (gzip) the bitmap from the ardb
func (sds *semiDedupedStorage) readBitMap() error {
	cmd := ardb.Command(command.Get, semiDedupBitMapKey(sds.vdiskID))
	bytes, err := ardb.Bytes(sds.cluster.Do(cmd))
	if err != nil {
		return err
	}
	return sds.userStorageBitMap.SetBytes(bytes)
}

// writeBitMap compresses and writes (gzip) the bitmap to the ardb
func (sds *semiDedupedStorage) writeBitMap() error {
	bytes, err := sds.userStorageBitMap.Bytes()
	if err != nil {
		return err
	}

	cmd := ardb.Command(command.Set, semiDedupBitMapKey(sds.vdiskID), bytes)
	return ardb.Error(sds.cluster.Do(cmd))
}

// semiDedupedVdiskExists checks if a semi deduped vdisks exists on a given cluster
func semiDedupedVdiskExists(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
	// first check if deduped content exists (probably does if this semi-deduped vdisk exists)
	exists, err := dedupedVdiskExists(vdiskID, cluster)
	if exists || err != nil {
		return exists, err
	}

	// no deduped vdisk exists, this probably means that the semi-deduped vdisk does not exists.
	// before we give up however, let's try if the semi deduped bitmap exists,
	// if it does, than somehow it does exists, with only non-deduped content.
	log.Infof(
		"checking if semi-deduped vdisk %s's bitmap exists on first available server",
		vdiskID)
	command := ardb.Command(command.Exists, semiDedupBitMapKey(vdiskID))
	return ardb.Bool(cluster.Do(command))
}

// deleteSemiDedupedData deletes the semi-deduped data of a given vdisk from a given cluster.
func deleteSemiDedupedData(vdiskID string, cluster ardb.StorageCluster) (bool, error) {
	// delete deduped data
	deletedDeduped, err := deleteDedupedData(vdiskID, cluster)
	if err != nil {
		return false, err
	}

	// delete nondeduped bitmap
	command := ardb.Command(command.Delete, semiDedupBitMapKey(vdiskID))
	deleted, err := ardb.Bool(cluster.Do(command))
	if err != nil {
		return false, err
	}
	if !deleted {
		return deletedDeduped, nil
	}

	// we have deleted the semi deduped bitmap,
	// so now let's also delete the non-deduped content.
	return deleteNonDedupedData(vdiskID, cluster)
}

// listSemiDedupedBlockIndices lists all the block indices (sorted)
// from a semi-deduped vdisk stored on a given cluster.
func listSemiDedupedBlockIndices(vdiskID string, cluster ardb.StorageCluster) ([]int64, error) {
	// get deduped' indices
	indices, err := listDedupedBlockIndices(vdiskID, cluster)
	if err != nil {
		return nil, err
	}

	// try to get nondeduped' indices
	ndIndices, err := listNonDedupedBlockIndices(vdiskID, cluster)
	if errors.Cause(err) == ardb.ErrNil {
		// no nondeduped' (user) indices found,
		// so early exit with a sorted slice containing only deduped' indices
		sortInt64s(indices)
		return indices, nil
	}
	if err != nil {
		return nil, err
	}

	// add both slices together,
	// sort them and dedup the total slice.
	indices = append(indices, ndIndices...)
	sortInt64s(indices)
	indices = dedupInt64s(indices)
	return indices, nil
}

// copySemiDeduped copies a semi deduped storage
// within the same or between different storage clusters.
func copySemiDeduped(sourceID, targetID string, sourceBS, targetBS int64, sourceCluster, targetCluster ardb.StorageCluster) error {
	err := copyDedupedMetadata(sourceID, targetID, sourceBS, targetBS, sourceCluster, targetCluster)
	if err != nil {
		return err
	}

	var hasBitMask bool
	if isInterfaceValueNil(targetCluster) {
		log.Infof(
			"copying semi-deduped metadata from vdisk %s to vdisk %s within a single storage cluster...",
			sourceID, targetID)
		hasBitMask, err = copySemiDedupedSingleCluster(sourceID, targetID, sourceCluster)
	} else {
		log.Infof(
			"copying semi-deduped metadata from vdisk %s to vdisk %s between storage clusters...",
			sourceID, targetID)
		hasBitMask, err = copySemiDedupedBetweenClusters(sourceID, targetID, sourceCluster, targetCluster)
	}
	if err != nil || !hasBitMask {
		return err
	}

	return copyNonDedupedData(sourceID, targetID, sourceBS, targetBS, sourceCluster, targetCluster)
}

func copySemiDedupedSingleCluster(sourceID, targetID string, cluster ardb.StorageCluster) (bool, error) {
	sourceKey := semiDedupBitMapKey(sourceID)
	targetkey := semiDedupBitMapKey(targetID)

	log.Debugf("copy semi-deduped bitmask from %s to %s on same cluster",
		sourceKey, targetkey)

	action := ardb.Script(0, copySemiDedupedMetaDataSameServerScriptSource,
		nil, sourceKey, targetkey)

	return ardb.Bool(cluster.Do(action))
}

func copySemiDedupedBetweenClusters(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcChan, err := sourceCluster.ServerIterator(ctx)
	if err != nil {
		return false, err
	}
	dstChan, err := targetCluster.ServerIterator(ctx)
	if err != nil {
		return false, err
	}

	src := <-srcChan
	dst := <-dstChan

	if srcConfig := src.Config(); srcConfig.Equal(dst.Config()) {
		log.Debugf(
			"copy semi-deduped bitmask from vdisk %s to vdisk %s on same server",
			sourceID, targetID)
		return copySemiDedupedSameServer(sourceID, targetID, src)
	}

	log.Debugf(
		"copy semi-deduped bitmask from vdisk %s to vdisk %s between different servers",
		sourceID, targetID)
	return copySemiDedupedDifferentServers(sourceID, targetID, src, dst)
}

func copySemiDedupedSameServer(sourceID, targetID string, server ardb.StorageServer) (bool, error) {
	sourceKey := semiDedupBitMapKey(sourceID)
	targetkey := semiDedupBitMapKey(targetID)

	log.Debugf("copy semi-deduped bitmask from %s to %s on same server %s",
		sourceKey, targetkey, server.Config())

	action := ardb.Script(0, copySemiDedupedMetaDataSameServerScriptSource,
		nil, sourceKey, targetkey)

	return ardb.Bool(server.Do(action))
}

func copySemiDedupedDifferentServers(sourceID, targetID string, src, dst ardb.StorageServer) (bool, error) {
	log.Debugf("collecting semi-deduped bitmask from %s for source vdisk %s...",
		src.Config(), sourceID)
	action := ardb.Command(command.Get, semiDedupBitMapKey(sourceID))
	bytes, err := ardb.Bytes(src.Do(action))
	if errors.Cause(err) == ardb.ErrNil {
		log.Debugf("no semi-deduped bitmask found for source vdisk %q...", sourceID)
		return false, nil
	}
	if err != nil {
		return false, err // couldn't get bitmask due to an unexpected error
	}

	log.Debugf("storing semi-deduped bitmask on %s for target vdisk %s...",
		dst.Config(), targetID)
	action = ardb.Command(command.Set, semiDedupBitMapKey(targetID), bytes)
	err = ardb.Error(dst.Do(action))
	return true, err
}

// semiDedupBitMapKey returns the storage key which is used
// to store the BitMap for the semideduped storage of a given vdisk
func semiDedupBitMapKey(vdiskID string) string {
	return semiDedupBitMapKeyPrefix + vdiskID
}

const (
	// semiDedupBitMapKeyPrefix is the prefix used in semiDedupBitMapKey
	semiDedupBitMapKeyPrefix = "semidedup:bitmap:"
)

const copySemiDedupedMetaDataSameServerScriptSource = `
local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", source) == 0 then
    return 0
end

if redis.call("EXISTS", destination) == 1 then
    redis.call("DEL", destination)
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))
return 1
`
