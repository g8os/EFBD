package storage

import (
	"bytes"
	crand "crypto/rand"
	mrand "math/rand"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage/lba"
	"github.com/zero-os/0-Disk/redisstub"
)

// simplified algorithm based on `cmd/copyvdisk/copy_different.go`
func copyTestMetaData(t *testing.T, vdiskIDA, vdiskIDB string, providerA, providerB ardb.MetadataConnProvider) {
	connA, err := providerA.MetadataConnection()
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer connA.Close()
	connB, err := providerB.MetadataConnection()
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer connB.Close()

	data, err := redis.StringMap(connA.Do("HGETALL", lba.StorageKey(vdiskIDA)))
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}

	_, err = connB.Do("DEL", lba.StorageKey(vdiskIDB))
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}

	var index int64
	for rawIndex, hash := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		_, err = connB.Do("HSET", lba.StorageKey(vdiskIDB), index, []byte(hash))
		if err != nil {
			debug.PrintStack()
			t.Fatal(err)
		}
	}
}

// testDedupContentExists tests if
// the given content exists in the database
func testDedupContentExists(t *testing.T, provider ardb.DataConnProvider, content []byte) {
	conn, err := provider.DataConnection(0)
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := zerodisk.HashBytes(content)

	contentReceived, err := redis.Bytes(conn.Do("GET", hash.Bytes()))
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	if bytes.Compare(content, contentReceived) != 0 {
		debug.PrintStack()
		t.Fatalf(
			"content found (%v) does not equal content expected (%v)",
			contentReceived, content)
	}
}

// testDedupContentDoesNotExist tests if
// the given content does not exist in the database
func testDedupContentDoesNotExist(t *testing.T, provider ardb.DataConnProvider, content []byte) {
	conn, err := provider.DataConnection(0)
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := zerodisk.HashBytes(content)

	exists, err := redis.Bool(conn.Do("EXISTS", hash.Bytes()))
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	if exists {
		debug.PrintStack()
		t.Fatalf(
			"content found (%v), while it shouldn't exist", content)
	}
}

func TestDedupedContent(t *testing.T) {
	const (
		vdiskID = "a"
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := Deduped(vdiskID, 8, ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorage(t, storage)
}

func TestDedupedContentForceFlush(t *testing.T) {
	const (
		vdiskID = "a"
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := Deduped(vdiskID, 8, ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorageForceFlush(t, storage)
}

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestDedupedDeadlock(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := Deduped(
		vdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorageDeadlock(t, blockSize, blockCount, storage)
}

// test if content linked to (copied) metadata,
// is available only on the template storage,
// while not yet on the primary storage
func TestGetPrimaryOrTemplateContent(t *testing.T) {
	const (
		vdiskIDA = "a"
		vdiskIDB = "b"
	)

	redisProviderA := redisstub.NewInMemoryRedisProvider(nil)
	storageA, err := Deduped(
		vdiskIDA, 8, ardb.DefaultLBACacheLimit, true, redisProviderA)
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	redisProviderB := redisstub.NewInMemoryRedisProvider(redisProviderA)
	storageB, err := Deduped(
		vdiskIDB, 8, ardb.DefaultLBACacheLimit, true, redisProviderB)
	if err != nil || storageB == nil {
		t.Fatalf("storageB could not be created: %v", err)
	}

	testContent := []byte{4, 2}

	// content shouldn't exist in either of the 2 volumes
	testDedupContentDoesNotExist(t, redisProviderA, testContent)
	testDedupContentDoesNotExist(t, redisProviderB, testContent)

	var testBlockIndex int64 // 0

	// store content in storageA
	err = storageA.SetBlock(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageA, but not yet in storageB
	testDedupContentExists(t, redisProviderA, testContent)
	testDedupContentDoesNotExist(t, redisProviderB, testContent)

	// let's flush to ensure metadata is written to external storage
	err = storageA.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// getting content from storageA should be possible
	content, err := storageA.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist, while received nil-content")
	}

	// getting content from StorageB isn't possible yet,
	// as metadata isn't copied yet
	content, err = storageB.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("content shouldn't exist yet, while received: %v", content)
	}
	// content should still only exist in storageA
	testDedupContentExists(t, redisProviderA, testContent)
	testDedupContentDoesNotExist(t, redisProviderB, testContent)

	// flush metadata of storageB first,
	// so it's reloaded next time fresh from externalStorage,
	// this shows that copying metadata to a vdisk which is active,
	// is only going lead to dissapointment
	err = storageB.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// copy metadata
	copyTestMetaData(t, vdiskIDA, vdiskIDB, redisProviderA, redisProviderB)

	// getting the content now should work
	content, err = storageB.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// content should now be in both storages
	// as the template content should also be in primary storage
	testDedupContentExists(t, redisProviderA, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testDedupContentExists(t, redisProviderB, testContent)

	// let's store some new content in storageB
	testContent = []byte{9, 2}
	testBlockIndex++

	err = storageB.SetBlock(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageB, but not yet in storageA
	testDedupContentExists(t, redisProviderB, testContent)
	testDedupContentDoesNotExist(t, redisProviderA, testContent)

	// let's flush to ensure metadata is written to external storage
	err = storageB.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// flush metadata of storageA first,
	// so it's reloaded next time fresh from externalStorage,
	// this shows that copying metadata to a vdisk which is active,
	// is only going lead to dissapointment
	err = storageA.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// let's copy the metadata from storageB to storageA
	copyTestMetaData(t, vdiskIDB, vdiskIDA, redisProviderB, redisProviderA)

	// let's now try to get it from storageA
	// this should fail (manifested as nil-content), as storageA has no template,
	// and the content isn't available in primary storage
	content, err = storageA.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("content shouldn't exist now, while received: %v", content)
	}

	// we can however get the content from storageB
	content, err = storageB.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// and we can also do our direct test to ensure the content
	// only exists in storageB
	testDedupContentExists(t, redisProviderB, testContent)
	testDedupContentDoesNotExist(t, redisProviderA, testContent)

	// if we now make sure storageA, has storageB as its template,
	// our previous Get attempt /will/ work, as we already have the metadata
	redisProviderA.SetTemplatePool(redisProviderB)

	content, err = storageA.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// and also our direct test should show that
	// the content now exists in both storages
	testDedupContentExists(t, redisProviderB, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testDedupContentExists(t, redisProviderA, testContent)
}

// test feature implemented for
// https://github.com/zero-os/0-Disk/issues/369
func TestDedupedStorageTemplateServerDown(t *testing.T) {
	const (
		vdiskIDA   = "a"
		vdiskIDB   = "b"
		blockSize  = 8
		blockCount = 8
	)

	var (
		err error
	)

	redisProviderA := redisstub.NewInMemoryRedisProvider(nil)
	storageA, err := Deduped(
		vdiskIDA, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProviderA)
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	redisProviderB := redisstub.NewInMemoryRedisProvider(redisProviderA)
	storageB, err := Deduped(
		vdiskIDB, blockSize,
		ardb.DefaultLBACacheLimit, true, redisProviderB)
	if err != nil || storageB == nil {
		t.Fatalf("storageB could not be created: %v", err)
	}

	someContent := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	someIndex := int64(0)

	someContentPlusOne := []byte{2, 0, 0, 0, 0, 0, 0, 0}
	someIndexPlusOne := someIndex + 1

	// write some content to the template storage (storageA)
	err = storageA.SetBlock(someIndex, someContent)
	if err != nil {
		t.Fatalf("could not write template content for index %d: %v", someIndex, err)
	}
	err = storageA.SetBlock(someIndexPlusOne, someContentPlusOne)
	if err != nil {
		t.Fatalf("could not write template content for index %d: %v", someIndexPlusOne, err)
	}
	err = storageA.Flush()
	if err != nil {
		t.Fatalf("couldn't flush template content: %v", err)
	}

	// let's copy the metadata from storageA to storageB
	copyTestMetaData(t, vdiskIDA, vdiskIDB, redisProviderA, redisProviderB)

	// now get that content in storageB, should be possible
	content, err := storageB.GetBlock(someIndex)
	if err != nil {
		t.Fatalf("could not read template content at index %d: %v", someIndex, err)
	}
	if !bytes.Equal(someContent, content) {
		t.Fatalf("invalid content for block index %d: %v", someIndex, content)
	}

	// now mark template invalid, and that should make it return an expected error instead
	redisProviderB.DisableTemplateConnection(0)
	content, err = storageB.GetBlock(someIndexPlusOne)
	if len(content) != 0 {
		t.Fatalf("content should be empty but was was: %v",
			content)
	}
	if err != ardb.ErrServerMarkedInvalid {
		t.Fatalf("error should be '%v', but instead was: %v",
			ardb.ErrServerMarkedInvalid, err)
	}
}

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestGetDedupedTemplateContentDeadlock(t *testing.T) {
	const (
		vdiskIDA   = "a"
		vdiskIDB   = "b"
		blockSize  = 128
		blockCount = 256
	)

	var (
		err error
	)

	redisProviderA := redisstub.NewInMemoryRedisProvider(nil)
	storageA, err := Deduped(
		vdiskIDA, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProviderA)
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	redisProviderB := redisstub.NewInMemoryRedisProvider(redisProviderA)
	storageB, err := Deduped(
		vdiskIDB, blockSize,
		ardb.DefaultLBACacheLimit, true, redisProviderB)
	if err != nil || storageB == nil {
		t.Fatalf("storageB could not be created: %v", err)
	}

	var contentArray [blockCount][]byte

	// store a lot of content in storageA
	for i := int64(0); i < blockCount; i++ {
		contentArray[i] = make([]byte, blockSize)
		crand.Read(contentArray[i])
		err = storageA.SetBlock(i, contentArray[i])
		if err != nil {
			t.Fatal(i, err)
		}
	}

	// flush metadata of storageA first,
	// so it's reloaded next time fresh from externalStorage,
	// this shows that copying metadata to a vdisk which is active,
	// is only going lead to dissapointment
	err = storageA.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// let's copy the metadata from storageA to storageB
	copyTestMetaData(t, vdiskIDA, vdiskIDB, redisProviderA, redisProviderB)

	// now restore all content
	// which will test the deadlock of async restoring
	for i := int64(0); i < blockCount; i++ {
		content, err := storageB.GetBlock(i)
		if err != nil {
			t.Fatal(i, err)
		}
		if bytes.Compare(contentArray[i], content) != 0 {
			t.Fatalf("unexpected content (%d): found %v, expected %v",
				i, content, contentArray[i])
		}
	}
}

/*
func TestDedupedVdiskExists(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 8
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	address := redisProvider.Address()
	clusterConfig := config.StorageClusterConfig{
		DataStorage: []config.StorageServerConfig{
			config.StorageServerConfig{
				Address: address,
			},
		},
		MetadataStorage: &config.StorageServerConfig{
			Address: address,
		},
	}

	storage, err := Deduped(
		vdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	exists, err := DedupedVdiskExists(vdiskID, &clusterConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("vdisk shouldn't exist yet while it does")
	}

	data := make([]byte, blockSize)
	_, err = rand.Read(data)
	if err != nil {
		t.Fatalf("couldn't generate a random block (#0): %v", err)
	}
	err = storage.SetBlock(0, data)
	if err != nil {
		t.Fatalf("couldn't set block #0: %v", err)
	}
	err = storage.Flush()
	if err != nil {
		t.Fatalf("couldn't flush storage: %v", err)
	}

	exists, err = DedupedVdiskExists(vdiskID, &clusterConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fmt.Println(redisProvider.Address())
	time.Sleep(time.Second * 300)
	if !exists {
		t.Fatal("vdisk should exist now, but it doesn't")
	}

	err = storage.DeleteBlock(0)
	if err != nil {
		t.Fatalf("couldn't delete block #0: %v", err)
	}
	err = storage.Flush()
	if err != nil {
		t.Fatalf("couldn't flush storage: %v", err)
	}

	exists, err = DedupedVdiskExists(vdiskID, &clusterConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("vdisk shouldn't exist anymore, but it does")
	}
}*/

// This test tests both if the amount of block indices listed for a deduped vdisk is correct,
// as well as the fact that all block indices are always in sorted order from small to big.
func TestListDedupedBlockIndices(t *testing.T) {
	const (
		vdiskID            = "a"
		blockSize          = 8
		blockCount         = 16
		blockIndexInterval = lba.NumberOfRecordsPerLBASector / 3
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	address := redisProvider.PrimaryAddress()
	clusterConfig := config.StorageClusterConfig{
		Servers: []config.StorageServerConfig{
			config.StorageServerConfig{
				Address: address,
			},
		},
	}

	storage, err := Deduped(
		vdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	indices, err := ListDedupedBlockIndices(vdiskID, &clusterConfig)
	if err == nil {
		t.Fatalf("expected an error, as no indices exist yet: %v", indices)
	}

	var expectedIndices []int64
	for i := 0; i < blockCount; i++ {
		// store a block
		data := make([]byte, blockSize)
		_, err := crand.Read(data)
		if err != nil {
			t.Fatalf("couldn't generate a random block (%d): %v", i, err)
		}

		blockIndex := int64(i * i * blockIndexInterval)
		expectedIndices = append(expectedIndices, blockIndex)
		err = storage.SetBlock(blockIndex, data)
		if err != nil {
			t.Fatalf("couldn't set block (%d): %v", i, err)
		}

		err = storage.Flush()
		if err != nil {
			t.Fatalf("couldn't flush storage (step %d): %v", i, err)
		}

		fetchedData, err := storage.GetBlock(blockIndex)
		if err != nil {
			t.Fatalf("couldn't get block (%d): %v", blockIndex, err)
		}
		if !assert.Equal(t, data, fetchedData) {
			t.Fatalf("couldn't get correct block %d", blockIndex)
		}

		// now test if listing the indices is correct
		indices, err := ListDedupedBlockIndices(vdiskID, &clusterConfig)
		if err != nil {
			t.Fatalf("couldn't list deduped block indices (step %d): %v", i, err)
		}
		if assert.Len(t, indices, len(expectedIndices)) {
			assert.Equal(t, expectedIndices, indices)
		}
	}

	// delete all odd counters
	ci := 1
	for i := 1; i < blockCount; i += 2 {
		blockIndex := int64(i * i * blockIndexInterval)

		err = storage.DeleteBlock(blockIndex)
		if err != nil {
			t.Fatalf("couldn't delete block %d: %v", i, err)
		}

		err = storage.Flush()
		if err != nil {
			t.Fatalf("couldn't flush storage (step %d): %v", i, err)
		}

		fetchedData, err := storage.GetBlock(blockIndex)
		if err != nil {
			t.Fatalf("couldn't get nil block (%d): %v", blockIndex, err)
		}
		if !assert.Empty(t, fetchedData) {
			t.Fatalf("block %d wasn't deleted yet", blockIndex)
		}

		expectedIndices = append(expectedIndices[:ci], expectedIndices[ci+1:]...)

		// now test if listing the indices is still correct
		indices, err := ListDedupedBlockIndices(vdiskID, &clusterConfig)
		if err != nil {
			t.Fatalf("couldn't list deduped block indices (step %d): %v", i, err)
		}

		if assert.Len(t, indices, len(expectedIndices), "at cut index %v", i) {
			assert.Equal(t, expectedIndices, indices, "at cut index %v", i)
		}

		ci++
	}
}

func TestCopyDedupedDifferentServerCount(t *testing.T) {
	assert := assert.New(t)

	sourceCluster := redisstub.NewInMemoryRedisProviderMultiServers(4, 0)
	if !assert.NotNil(sourceCluster) {
		return
	}
	defer sourceCluster.Close()

	sourceProvider, err := ardb.StaticProviderFromStorageCluster(*sourceCluster.ClusterConfig(), nil)
	if !assert.NoError(err) {
		return
	}
	defer sourceProvider.Close()

	sourceSectorStorage := lba.ARDBSectorStorage("source", sourceProvider)

	// create random source sectors
	var indices []int64
	for i := 0; i < 128; i++ {
		bytes := make([]byte, lba.BytesPerSector)
		crand.Read(bytes)

		sector, err := lba.SectorFromBytes(bytes)
		if !assert.NoError(err) {
			return
		}

		index := int64(mrand.Uint32())
		indices = append(indices, index)

		sourceSectorStorage.SetSector(index, sector)
	}

	// test copying to a target cluster with less servers available
	testCopyDedupedDifferentServerCount(assert, indices, "source", "target",
		sourceCluster.ClusterConfig(), sourceSectorStorage, 3)
	testCopyDedupedDifferentServerCount(assert, indices, "source", "target",
		sourceCluster.ClusterConfig(), sourceSectorStorage, 2)
	testCopyDedupedDifferentServerCount(assert, indices, "source", "target",
		sourceCluster.ClusterConfig(), sourceSectorStorage, 1)

	// test coping to a target cluster with more servers available
	testCopyDedupedDifferentServerCount(assert, indices, "source", "target",
		sourceCluster.ClusterConfig(), sourceSectorStorage, 5)
	testCopyDedupedDifferentServerCount(assert, indices, "source", "target",
		sourceCluster.ClusterConfig(), sourceSectorStorage, 8)
}

func testCopyDedupedDifferentServerCount(assert *assert.Assertions, indices []int64, sourceID, targetID string, sourceCluster *config.StorageClusterConfig, sourceSectorStorage lba.SectorStorage, targetServerCount int) {
	targetCluster := redisstub.NewInMemoryRedisProviderMultiServers(targetServerCount, 0)
	if !assert.NotNil(targetCluster) {
		return
	}
	defer targetCluster.Close()

	// copy the sectors
	err := copyDedupedDifferentServerCount(sourceID, targetID, sourceCluster, targetCluster.ClusterConfig())
	if !assert.NoError(err) {
		return
	}

	// now validate all sectors are correctly copied

	targetProvider, err := ardb.StaticProviderFromStorageCluster(*targetCluster.ClusterConfig(), nil)
	if !assert.NoError(err) {
		return
	}
	defer targetProvider.Close()

	targetSectorStorage := lba.ARDBSectorStorage(targetID, targetProvider)
	for _, index := range indices {
		sourceSector, err := sourceSectorStorage.GetSector(index)
		if !assert.NoError(err) {
			return
		}
		targetSector, err := targetSectorStorage.GetSector(index)
		if !assert.NoError(err) {
			return
		}

		if !assert.Equal(sourceSector.Bytes(), targetSector.Bytes()) {
			return
		}
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
