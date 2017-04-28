package ardb

import (
	"bytes"
	"context"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/g8os/blockstor"
	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/redisstub"
	"github.com/garyburd/redigo/redis"
)

// simplified algorithm based on `cmd/copyvdisk/copy_different.go`
func copyTestMetaData(t *testing.T, vdiskIDA, vdiskIDB string, providerA, providerB lba.MetaRedisProvider) {
	connA, err := providerA.MetaRedisConnection()
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer connA.Close()
	connB, err := providerB.MetaRedisConnection()
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer connB.Close()

	data, err := redis.StringMap(connA.Do("HGETALL", vdiskIDA))
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}

	_, err = connB.Do("DEL", vdiskIDB)
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

		_, err = connB.Do("HSET", vdiskIDB, index, []byte(hash))
		if err != nil {
			debug.PrintStack()
			t.Fatal(err)
		}
	}
}

func createTestDedupedStorage(t *testing.T, vdiskID string, blockSize, blockCount int64, provider *testRedisProvider) *dedupedStorage {
	lba, err := lba.NewLBA(
		vdiskID,
		blockCount,
		DefaultLBACacheLimit,
		provider)
	if err != nil {
		t.Fatal("couldn't create LBA", err)
	}

	return newDedupedStorage(vdiskID, blockSize, provider, lba).(*dedupedStorage)
}

// testDedupContentExists tests if
// the given content exists in the database
func testDedupContentExists(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte) {
	conn, err := memRedis.Dial("")
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := blockstor.HashBytes(content)

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
func testDedupContentDoesNotExist(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte) {
	conn, err := memRedis.Dial("")
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := blockstor.HashBytes(content)

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

// testReferenceCount of a certain content
func testReferenceCount(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte, expected int64) {
	time.Sleep(time.Millisecond * 200) // give background thread time
	conn, err := memRedis.Dial("")
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := blockstor.HashBytes(content)
	key := dsReferenceKey(hash)
	length, err := redis.Int64(conn.Do("GET", key))
	if err == redis.ErrNil {
		length = 0
		err = nil
	}
	if err != nil {
		debug.PrintStack()
		t.Fatal("couldn't get reference count of", hash, err)
	}
	if expected != length {
		debug.PrintStack()
		t.Fatalf("%v has length %v, while expected %v", content, length, expected)
	}
}

func TestDedupedContent(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	var (
		ctx = context.Background()
	)

	redisProvider := &testRedisProvider{memRedis, nil} // root = nil
	storage := createTestDedupedStorage(t, vdiskID, 8, 8, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}
	defer storage.Close()
	go storage.GoBackground(ctx)

	testBackendStorage(t, storage)
}

// test if content linked to local (copied) metadata,
// is available only on the remote (root) storage,
// while not yet on the local storage
func TestGetDedupedRootContent(t *testing.T) {
	// create storageA
	memRedisA := redisstub.NewMemoryRedis()
	go memRedisA.Listen()
	defer memRedisA.Close()

	const (
		vdiskIDA = "a"
		vdiskIDB = "b"
	)

	var (
		ctx = context.Background()
	)

	redisProviderA := &testRedisProvider{memRedisA, nil} // root = nil
	storageA := createTestDedupedStorage(t, vdiskIDA, 8, 8, redisProviderA)
	if storageA == nil {
		t.Fatal("storageA is nil")
	}
	defer storageA.Close()
	go storageA.GoBackground(ctx)

	// create storageB, with storageA as its fallback/root
	memRedisB := redisstub.NewMemoryRedis()
	go memRedisB.Listen()
	defer memRedisB.Close()

	redisProviderB := &testRedisProvider{memRedisB, memRedisA} // root = memRedisA
	storageB := createTestDedupedStorage(t, vdiskIDB, 8, 8, redisProviderB)
	if storageB == nil {
		t.Fatal("storageB is nil")
	}
	defer storageB.Close()
	go storageB.GoBackground(ctx)

	testContent := []byte{4, 2}

	// content shouldn't exist in either of the 2 volumes
	testDedupContentDoesNotExist(t, memRedisA, testContent)
	testDedupContentDoesNotExist(t, memRedisB, testContent)

	var testBlockIndex int64 // 0

	// store content in storageA
	err := storageA.Set(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageA, but not yet in storageB
	testDedupContentExists(t, memRedisA, testContent)
	testDedupContentDoesNotExist(t, memRedisB, testContent)

	// let's flush to ensure metadata is written to external storage
	err = storageA.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// getting content from storageA should be possible
	content, err := storageA.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist, while received nil-content")
	}

	// getting content from StorageB isn't possible yet,
	// as metadata isn't copied yet
	content, err = storageB.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("content shouldn't exist yet, while received: %v", content)
	}
	// content should still only exist in storageA
	testDedupContentExists(t, memRedisA, testContent)
	testDedupContentDoesNotExist(t, memRedisB, testContent)

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
	content, err = storageB.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// content should now be in both storages
	// as the remote get should have also stored the content locally
	testDedupContentExists(t, memRedisA, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testDedupContentExists(t, memRedisB, testContent)

	// let's store some new content in storageB
	testContent = []byte{9, 2}
	testBlockIndex++

	err = storageB.Set(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageB, but not yet in storageA
	testDedupContentExists(t, memRedisB, testContent)
	testDedupContentDoesNotExist(t, memRedisA, testContent)

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
	// this should fail (manifested as nil-content), as storageA has no root,
	// and the content isn't available locally
	content, err = storageA.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("content shouldn't exist now, while received: %v", content)
	}

	// we can however get the content from storageB
	content, err = storageB.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// and we can also do our direct test to ensure the content
	// only exists in storageB
	testDedupContentExists(t, memRedisB, testContent)
	testDedupContentDoesNotExist(t, memRedisA, testContent)

	// if we now make sure storageA, has storageB as its root,
	// our previous Get attempt /will/ work, as we already have the metadata
	redisProviderA.rootMemRedis = memRedisB

	content, err = storageA.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// and also our direct test should show that
	// the content now exists in both storages
	testDedupContentExists(t, memRedisB, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testDedupContentExists(t, memRedisA, testContent)
}

// this test only runs when the `redis` flag is specified
func TestDedupedStorageReferenceCount(t *testing.T) {
	memoryRedis := redisstub.NewMemoryRedis()
	go memoryRedis.Listen()
	defer memoryRedis.Close()

	redisPool := NewRedisPool(memoryRedis.Dial)
	defer redisPool.Close()

	redisProvider := &testRedisProvider{memoryRedis, nil} // no root

	ctx := context.Background()

	storageA := createTestDedupedStorage(t, "a", 8, 8, redisProvider)
	defer storageA.Close()
	go storageA.GoBackground(ctx)

	contentA := []byte{4, 2}

	blockIndexA := int64(1)

	// set content in storageA
	err := storageA.Set(blockIndexA, contentA)
	if err != nil {
		t.Fatal("couldn't set content in storage A", err)
	}
	// reference Count should now be 1
	testReferenceCount(t, memoryRedis, contentA, 1)
	testDedupContentExists(t, memoryRedis, contentA)

	storageB := createTestDedupedStorage(t, "b", 8, 8, redisProvider)
	defer storageB.Close()
	go storageB.GoBackground(ctx)

	err = storageB.Set(blockIndexA, contentA)
	if err != nil {
		t.Fatal("couldn't set content in storage B", err)
	}
	// reference Count should now be 2
	testReferenceCount(t, memoryRedis, contentA, 2)
	testDedupContentExists(t, memoryRedis, contentA)

	// adding the content to a vdiskID that already has it,
	// should not increase the reference count
	for i := 0; i < 3; i++ {
		err = storageB.Set(blockIndexA, contentA)
		if err != nil {
			t.Fatal("couldn't set content in storage B", err)
		}
		// reference Count should still be 2
		testReferenceCount(t, memoryRedis, contentA, 2)
		testDedupContentExists(t, memoryRedis, contentA)
	}

	blockIndexB := int64(2)

	// adding the content to a vdiskID that already has it,
	// should not increase the reference count
	err = storageA.Set(blockIndexB, contentA)
	if err != nil {
		t.Fatal("couldn't set content in storage A", err)
	}
	// reference Count should be 3 now
	testReferenceCount(t, memoryRedis, contentA, 3)
	testDedupContentExists(t, memoryRedis, contentA)

	contentB := []byte{1, 2, 3}

	// set new content in new block
	err = storageB.Set(blockIndexB, contentB)
	if err != nil {
		t.Fatal("couldn't set content in storage B", err)
	}
	// reference Count of contentB should be 1, as it is new
	testReferenceCount(t, memoryRedis, contentB, 1)
	testDedupContentExists(t, memoryRedis, contentB)
	// reference Count of contentA should still be 3, as it shouldn't have been touched
	testReferenceCount(t, memoryRedis, contentA, 3)
	testDedupContentExists(t, memoryRedis, contentA)

	// overwrite block with new content
	err = storageA.Set(blockIndexA, contentB)
	if err != nil {
		t.Fatal("couldn't set content in storage A", err)
	}
	// reference Count of contentB should be 2 now, as both vdisks reference it
	testReferenceCount(t, memoryRedis, contentB, 2)
	testDedupContentExists(t, memoryRedis, contentB)
	// reference Count of contentA should now be 2, as storageA, no longer references it twice
	testReferenceCount(t, memoryRedis, contentA, 2)
	testDedupContentExists(t, memoryRedis, contentA)

	err = storageA.Delete(blockIndexA)
	if err != nil {
		t.Fatal("couldn't delete content from storage A", err)
	}
	// reference Count of contentB should be 1 now, as only storage A references it
	testReferenceCount(t, memoryRedis, contentB, 1)
	testDedupContentExists(t, memoryRedis, contentB)

	contentC := []byte{3, 2, 1}
	// merge contentA with contentC
	err = storageA.Merge(blockIndexB, 3, contentC)
	if err != nil {
		t.Fatal("couldn't merge content in storage A", err)
	}
	testReferenceCount(t, memoryRedis, contentA, 1)

	// get merged Content
	contentAPlusC, err := storageA.Get(blockIndexB)
	if err != nil {
		t.Fatal("couldn't get merged content from storage A", err)
	}
	if bytes.Compare(contentAPlusC, []byte{4, 2, 0, 3, 2, 1, 0, 0}) != 0 {
		t.Fatal("contentAPlusC is not correct", contentAPlusC)
	}
	testReferenceCount(t, memoryRedis, contentA, 1)
	testDedupContentExists(t, memoryRedis, contentA)

	testReferenceCount(t, memoryRedis, contentB, 1)
	testDedupContentExists(t, memoryRedis, contentB)

	testReferenceCount(t, memoryRedis, contentC, 0)
	testDedupContentDoesNotExist(t, memoryRedis, contentC)

	testReferenceCount(t, memoryRedis, contentAPlusC, 1)
	testDedupContentExists(t, memoryRedis, contentAPlusC)
}
