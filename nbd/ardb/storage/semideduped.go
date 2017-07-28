package storage

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
)

// SemiDeduped returns a semi deduped BlockStorage
func SemiDeduped(vdiskID string, vdiskSize, blockSize, lbaCacheLimit int64, provider ardb.ConnProvider) (BlockStorage, error) {
	templateStorage, err := Deduped(vdiskID, vdiskSize, blockSize, lbaCacheLimit, true, provider)
	if err != nil {
		return nil, err
	}

	userStorage, err := NonDeduped(vdiskID, "", blockSize, false, provider)
	if err != nil {
		templateStorage.Close()
		return nil, err
	}

	storage := &semiDedupedStorage{
		templateStorage: templateStorage,
		userStorage:     userStorage,
		vdiskID:         vdiskID,
		blockSize:       blockSize,
		provider:        provider,
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
	provider ardb.MetadataConnProvider

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
	tErr := sds.templateStorage.DeleteBlock(blockIndex)

	// note that we don't unset the storage bit from the bitmask,
	// as that would basically flip it back to use dedup storage for this index,
	// which is not something we want,
	// as from a user perspective that already has been overwritten
	uErr := sds.userStorage.DeleteBlock(blockIndex)

	return combineErrorPair(tErr, uErr)
}

// Flush implements BlockStorage.Flush
func (sds *semiDedupedStorage) Flush() error {
	tErr := sds.templateStorage.Flush()
	uErr := sds.userStorage.Flush()

	// serialize bitmap
	storageErr := combineErrorPair(tErr, uErr)
	bitmapErr := sds.writeBitMap()

	return combineErrorPair(storageErr, bitmapErr)
}

// Close implements BlockStorage.Close
func (sds *semiDedupedStorage) Close() error {
	tErr := sds.templateStorage.Close()
	uErr := sds.userStorage.Close()
	return combineErrorPair(tErr, uErr)
}

// readBitMap reads and decompresses (gzip) the bitmap from the ardb
func (sds *semiDedupedStorage) readBitMap() error {
	conn, err := sds.provider.MetadataConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	bytes, err := redis.Bytes(conn.Do("GET", semiDedupBitMapKey(sds.vdiskID)))
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

	conn, err := sds.provider.MetadataConnection()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", semiDedupBitMapKey(sds.vdiskID), bytes)
	return err
}

func combineErrorPair(e1, e2 error) error {
	if e1 == nil {
		return e2
	}

	if e2 == nil {
		return e1
	}

	return fmt.Errorf("%v; %v", e1, e2)
}

// CopySemiDeduped copies a semi deduped storage
// within the same or between different storage clusters.
func CopySemiDeduped(sourceID, targetID string, sourceCluster, targetCluster *config.StorageClusterConfig) error {
	// validate source cluster
	if sourceCluster == nil {
		return errors.New("no source cluster given")
	}
	if sourceCluster.MetadataStorage == nil {
		return errors.New("no metaDataServer given for source")
	}

	// define whether or not we're copying between different servers,
	// and if the target cluster is given, make sure to validate it.
	var sameMetadataServer bool
	if targetCluster == nil {
		sameMetadataServer = true
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
		sameMetadataServer = *sourceCluster.MetadataStorage == *targetCluster.MetadataStorage
	}

	var hasBitMask bool
	var err error

	// within same meta storage server
	if sameMetadataServer {
		// copy metadata from the same storage server
		hasBitMask, err = func() (hasBitMask bool, err error) {
			conn, err := ardb.GetConnection(*sourceCluster.MetadataStorage)
			if err != nil {
				err = fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
				return
			}
			defer conn.Close()

			// copy metadata of deduped metadata
			err = copyDedupedSameConnection(sourceID, targetID, conn)
			if err != nil {
				err = fmt.Errorf("couldn't copy deduped metadata: %s", err.Error())
				return
			}

			// copy bitmask
			hasBitMask, err = copySemiDedupedSameConnection(sourceID, targetID, conn)
			return
		}()
	} else {
		// copy metadata from different storage servers
		hasBitMask, err = func() (hasBitMask bool, err error) {
			conns, err := ardb.GetConnections(*sourceCluster.MetadataStorage, *targetCluster.MetadataStorage)
			if err != nil {
				err = fmt.Errorf("couldn't connect to meta ardb: %s", err.Error())
				return
			}
			defer func() {
				conns[0].Close()
				conns[1].Close()
			}()

			// copy metadata of deduped metadata
			err = copyDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
			if err != nil {
				err = fmt.Errorf("couldn't copy deduped metadata: %s", err.Error())
				return
			}

			// copy bitmask
			hasBitMask, err = copySemiDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
			return
		}()
	}

	if err != nil {
		return fmt.Errorf("couldn't copy bitmask: %v", err)
	}
	if !hasBitMask {
		// no bitmask == no nondeduped content,
		// which means this is an untouched semideduped storage
		return nil // nothing to do, early return
	}

	// dispatch the rest of the work to the CopyNonDeduped func,
	// to copy all the user (nondeduped) data
	err = CopyNonDeduped(sourceID, targetID, sourceCluster, targetCluster)
	if err != nil {
		return fmt.Errorf("couldn't copy nondeduped content: %v", err)
	}

	// copy went ALL-OK!
	return nil
}

// NOTE: copies bitmask only
func copySemiDedupedSameConnection(sourceID, targetID string, conn redis.Conn) (hasBitMask bool, err error) {
	script := redis.NewScript(0, `
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
`)

	log.Infof("dumping vdisk %q's bitmask and restoring it as vdisk %q's bitmask",
		sourceID, targetID)

	sourceKey := semiDedupBitMapKey(sourceID)
	targetKey := semiDedupBitMapKey(targetID)

	hasBitMask, err = redis.Bool(script.Do(conn, sourceKey, targetKey))
	return
}

// NOTE: copies bitmask only
func copySemiDedupedDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) (hasBitMask bool, err error) {
	sourceKey := semiDedupBitMapKey(sourceID)

	log.Infof("collecting semidedup bitmask from source vdisk %q...", sourceID)
	bytes, err := redis.Bytes(connA.Do("GET", sourceKey))
	if err == redis.ErrNil {
		err = nil
		log.Infof("no semidedup bitmask found for source vdisk %q...", sourceID)
		return // nothing to do, as there is no bitmask
	}
	if err != nil {
		return // couldn't get bitmask due to an unexpected error
	}

	log.Infof("collected semidedup bitmask from source vdisk %q...", sourceID)

	targetKey := semiDedupBitMapKey(targetID)
	_, err = connB.Do("SET", targetKey, bytes)
	if err != nil {
		return // couldn't set bitmask, this makes the vdisk invalid
	}

	log.Infof("stored semidedup bitmask for target storage %q...", targetID)

	hasBitMask = true
	return
}

func newDeleteSemiDedupedDataOp(vdiskID string) storageOp {
	return &deleteSemiDedupedDataOp{deleteNonDedupedDataOp{vdiskID}}
}

type deleteSemiDedupedDataOp struct {
	deleteNonDedupedDataOp
}

func (op *deleteSemiDedupedDataOp) Label() string {
	return "delete semideduped data of " + op.vdiskID
}

func newDeleteSemiDedupedMetaDataOp(vdiskID string) storageOp {
	return &deleteSemiDedupedMetaDataOp{
		vdiskID:         vdiskID,
		dedupedMetaData: newDeleteDedupedMetadataOp(vdiskID),
	}
}

type deleteSemiDedupedMetaDataOp struct {
	vdiskID         string
	dedupedMetaData storageOp
}

func (op *deleteSemiDedupedMetaDataOp) Send(sender storageOpSender) error {
	log.Debugf("batch deletion of semideduped metadata for: %v", op.vdiskID)
	err := sender.Send("DEL", semiDedupBitMapKey(op.vdiskID))
	if err != nil {
		return err
	}

	return op.dedupedMetaData.Send(sender)
}

func (op *deleteSemiDedupedMetaDataOp) Receive(receiver storageOpReceiver) error {
	var errs pipelineErrors

	_, errA := receiver.Receive()
	errs.AddError(errA)
	errs.AddError(op.dedupedMetaData.Receive(receiver))

	return errs
}

func (op *deleteSemiDedupedMetaDataOp) Label() string {
	return "delete semideduped metadata of " + op.vdiskID
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
