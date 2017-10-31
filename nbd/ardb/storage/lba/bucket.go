package lba

import (
	"container/list"
	"sync"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
)

// create a new sector bucket
// NOTE that this constructor doesn't validate its input
func newSectorBucket(sizeLimitInBytes int64, storage SectorStorage) *sectorBucket {
	// the amount of most recent sectors we keep in memory
	size := sizeLimitInBytes / BytesPerSector

	return &sectorBucket{
		sectors:   make(map[int64]*list.Element, size),
		evictList: list.New(),
		size:      int(size),
		storage:   storage,
	}
}

// sectorBucket contains a bucket of sectors,
// only the most recent sectors of this bucket are kept.
type sectorBucket struct {
	sectors   map[int64]*list.Element
	evictList *list.List
	size      int
	storage   SectorStorage
	mux       sync.Mutex
}

// SetHash sets a hash in the given bucket,
// which will end up in the persistent storage by auto-eviction,
// or because the Flush function of this bucket was explicitly called.
// When a nil is given as the hash, a full-zeroes hash will be written to its sector.
func (bucket *sectorBucket) SetHash(index int64, hash zerodisk.Hash) error {
	bucket.mux.Lock()
	defer bucket.mux.Unlock()

	sectorIndex := index / NumberOfRecordsPerLBASector
	sector, err := bucket.getSector(sectorIndex)
	if err != nil {
		return err
	}

	localIndex := index % NumberOfRecordsPerLBASector
	sector.Set(localIndex, hash)
	return nil
}

// GetHash gets (a copy of) a hash from the given bucket.
// The hash will be first fetched from the persistent storage,
// if its sector wasn't cached yet.
func (bucket *sectorBucket) GetHash(index int64) (zerodisk.Hash, error) {
	bucket.mux.Lock()
	defer bucket.mux.Unlock()

	// get the sector of the hash
	sectorIndex := index / NumberOfRecordsPerLBASector
	sector, err := bucket.getSector(sectorIndex)
	if err != nil {
		return nil, err
	}

	// sector found, so now get a copy of the hash
	// (if the hash was set in the first place)
	localIndex := index % NumberOfRecordsPerLBASector
	hash := sector.Get(localIndex)
	// NOTE: hash can be nil, for now we don't treat it as an error
	return hash, nil
}

// Flush all sectors from this bucket to persistent storage,
// after which its bucket will be empty.
func (bucket *sectorBucket) Flush() error {
	bucket.mux.Lock()
	defer bucket.mux.Unlock()

	var entry *cacheEntry

	var err error
	errs := errors.NewErrorSlice()

	// mark all sectors for flushing
	for elem := bucket.evictList.Front(); elem != nil; elem = elem.Next() {
		entry = elem.Value.(*cacheEntry)

		if !entry.sector.Dirty() {
			continue // only write dirty sectors
		}

		// mark the sector for persistent storage
		err = bucket.storage.SetSector(entry.sectorIndex, entry.sector)
		errs.Add(err)
	}

	// clear bucket, which is something we want to do always
	// as flushing is used only as a final action,
	// if it fails, the user knows something is wrong,
	// and shouldn't rely that any blocks that were written,
	// are also stored.
	bucket.sectors = make(map[int64]*list.Element, bucket.size)
	bucket.evictList.Init()

	// flush bucket storage
	return errs.AsError()
}

// getSector returns a cached or fresh sector.
func (bucket *sectorBucket) getSector(index int64) (*Sector, error) {
	// return the sector from bucket
	if elem, ok := bucket.sectors[index]; ok {
		bucket.evictList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).sector, nil
	}

	// try to fetch the sector from persistent storage
	sector, err := bucket.storage.GetSector(index)
	if err != nil {
		return nil, err
	}

	// sector found, keep it in bucket as well
	entry := bucket.evictList.PushFront(&cacheEntry{
		sectorIndex: index,
		sector:      sector,
	})
	bucket.sectors[index] = entry

	// Verify size not exceeded
	if bucket.evictList.Len() > bucket.size {
		// size exceeded, thus evict the oldest one
		err := bucket.flushOldest()
		if err != nil {
			log.Errorf("couldn't flush oldest cached sector: %v", err)
		}
	}

	// and return the found sector finally
	return sector, nil
}

// flush the oldest sector to persistent storage.
func (bucket *sectorBucket) flushOldest() error {
	if ent := bucket.evictList.Back(); ent != nil {
		// last entry found, so let's remove
		// its references and flush it to the
		// persistent storage
		bucket.evictList.Remove(ent)
		entry := ent.Value.(*cacheEntry)
		delete(bucket.sectors, entry.sectorIndex)

		return bucket.storage.SetSector(entry.sectorIndex, entry.sector)
	}

	return errBucketIsEmpty
}

var (
	errBucketIsEmpty = errors.New("bucket is empty")
)

// cacheEntry defines the entry for a bucket
type cacheEntry struct {
	sectorIndex int64
	sector      *Sector
}
