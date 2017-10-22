package storage

import (
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
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

func combineErrorPair(e1, e2 error) error {
	if e1 == nil {
		return e2
	}

	if e2 == nil {
		return e1
	}

	return fmt.Errorf("%v; %v", e1, e2)
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
	if err == ardb.ErrNil {
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

// CopySemiDeduped copies a semi deduped storage
// within the same or between different storage clusters.
func CopySemiDeduped(sourceID, targetID string, sourceCluster config.StorageClusterConfig, targetCluster *config.StorageClusterConfig) error {
	sourceDataServerCount := len(sourceCluster.Servers)
	if sourceDataServerCount == 0 {
		return errors.New("no data server configs given for source")
	}

	// define whether or not we're copying between different clusters,
	// and if the target cluster is given, make sure to validate it.
	if targetCluster == nil {
		targetCluster = &sourceCluster
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

	metaSourceCfg, err := ardb.FindFirstAvailableServerConfig(sourceCluster)
	if err != nil {
		return err
	}
	metaTargetCfg, err := ardb.FindFirstAvailableServerConfig(*targetCluster)
	if err != nil {
		return err
	}

	var hasBitMask bool
	if metaSourceCfg.Equal(metaTargetCfg) {
		hasBitMask, err = func() (bool, error) {
			conn, err := ardb.Dial(metaSourceCfg)
			if err != nil {
				return false, fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
			}
			defer conn.Close()

			return copySemiDedupedSameConnection(sourceID, targetID, conn)
		}()
	} else {
		hasBitMask, err = func() (bool, error) {
			conns, err := ardb.DialAll(metaSourceCfg, metaTargetCfg)
			if err != nil {
				return false, fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
			}
			defer func() {
				conns[0].Close()
				conns[1].Close()
			}()

			return copySemiDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
		}()
	}

	var sourceCfg, targetCfg config.StorageServerConfig

	for i := 0; i < sourceDataServerCount; i++ {
		sourceCfg = sourceCluster.Servers[i]
		targetCfg = targetCluster.Servers[i]

		if sourceCfg.Equal(targetCfg) {
			// within same storage server
			err = func() error {
				conn, err := ardb.Dial(sourceCfg)
				if err != nil {
					return fmt.Errorf("couldn't connect to data ardb: %s", err.Error())
				}
				defer conn.Close()

				err = copyDedupedSameConnection(sourceID, targetID, conn)
				if err != nil {
					return fmt.Errorf("couldn't copy deduped data on same connection: %v", err)
				}

				if hasBitMask {
					err = copyNonDedupedSameConnection(sourceID, targetID, conn)
					if err != nil {
						return fmt.Errorf("couldn't copy non-deduped (meta)data on same connection: %v", err)
					}
				}

				return nil
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

				err = copyDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
				if err != nil {
					return fmt.Errorf("couldn't copy deduped data between connections: %v", err)
				}

				if hasBitMask {
					err = copyNonDedupedDifferentConnections(sourceID, targetID, conns[0], conns[1])
					if err != nil {
						return fmt.Errorf("couldn't copy non-deduped (meta)data between connections: %v", err)
					}
				}

				return nil
			}()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// NOTE: copies bitmask only
func copySemiDedupedSameConnection(sourceID, targetID string, conn ardb.Conn) (hasBitMask bool, err error) {
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

	hasBitMask, err = ardb.Bool(script.Do(conn, sourceKey, targetKey))
	return
}

// NOTE: copies bitmask only
func copySemiDedupedDifferentConnections(sourceID, targetID string, connA, connB ardb.Conn) (hasBitMask bool, err error) {
	sourceKey := semiDedupBitMapKey(sourceID)

	log.Infof("collecting semidedup bitmask from source vdisk %q...", sourceID)
	bytes, err := ardb.Bytes(connA.Do("GET", sourceKey))
	if err == ardb.ErrNil {
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

// semiDedupBitMapKey returns the storage key which is used
// to store the BitMap for the semideduped storage of a given vdisk
func semiDedupBitMapKey(vdiskID string) string {
	return semiDedupBitMapKeyPrefix + vdiskID
}

const (
	// semiDedupBitMapKeyPrefix is the prefix used in semiDedupBitMapKey
	semiDedupBitMapKeyPrefix = "semidedup:bitmap:"
)
