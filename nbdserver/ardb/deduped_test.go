package ardb

import (
	"bytes"
	"context"
	"crypto/rand"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/lba"
	"github.com/zero-os/0-Disk/redisstub"
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

func createTestDedupedStorage(t *testing.T, vdiskID string, blockSize, blockCount int64, templateSupport bool, provider *testRedisProvider) *dedupedStorage {
	lba, err := lba.NewLBA(
		vdiskID,
		blockCount,
		DefaultLBACacheLimit,
		provider)
	if err != nil {
		t.Fatal("couldn't create LBA", err)
	}

	return newDedupedStorage(vdiskID, blockSize, provider, templateSupport, lba).(*dedupedStorage)
}

// testDedupContentExists tests if
// the given content exists in the database
func testDedupContentExists(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte) {
	conn, err := memRedis.Dial("", 0)
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
func testDedupContentDoesNotExist(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte) {
	conn, err := memRedis.Dial("", 0)
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
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // template = nil
	storage := createTestDedupedStorage(t, vdiskID, 8, 8, false, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorage(t, storage)
}

func TestDedupedContentForceFlush(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // template = nil
	storage := createTestDedupedStorage(t, vdiskID, 8, 8, false, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorageForceFlush(t, storage)
}

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestDedupedDeadlock(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // template = nil
	storage := createTestDedupedStorage(t, vdiskID, blockSize, blockCount, false, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorageDeadlock(t, blockSize, blockCount, storage)
}

// test if content linked to (copied) metadata,
// is available only on the template storage,
// while not yet on the primary storage
func TestGetPrimaryOrTemplateContent(t *testing.T) {
	// create storageA
	memRedisA := redisstub.NewMemoryRedis()
	go memRedisA.Listen()
	defer memRedisA.Close()

	const (
		vdiskIDA = "a"
		vdiskIDB = "b"
	)

	redisProviderA := newTestRedisProvider(memRedisA, nil) // template = nil, later will be non-nil
	storageA := createTestDedupedStorage(t, vdiskIDA, 8, 8, true, redisProviderA)
	if storageA == nil {
		t.Fatal("storageA is nil")
	}

	// create storageB, with storageA as its template storage
	memRedisB := redisstub.NewMemoryRedis()
	go memRedisB.Listen()
	defer memRedisB.Close()

	redisProviderB := newTestRedisProvider(memRedisB, memRedisA) // template = memRedisA
	storageB := createTestDedupedStorage(t, vdiskIDB, 8, 8, true, redisProviderB)
	if storageB == nil {
		t.Fatal("storageB is nil")
	}

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
	// as the template content should also be in primary storage
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
	// this should fail (manifested as nil-content), as storageA has no template,
	// and the content isn't available in primary storage
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

	// if we now make sure storageA, has storageB as its template,
	// our previous Get attempt /will/ work, as we already have the metadata
	redisProviderA.templateMemRedis = memRedisB

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

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestGetDedupedTemplateContentDeadlock(t *testing.T) {
	// create storageA
	memRedisA := redisstub.NewMemoryRedis()
	go memRedisA.Listen()
	defer memRedisA.Close()

	const (
		vdiskIDA   = "a"
		vdiskIDB   = "b"
		blockSize  = 128
		blockCount = 256
	)

	var (
		err error
	)

	redisProviderA := newTestRedisProvider(memRedisA, nil) // template = nil, later will be non-nil
	storageA := createTestDedupedStorage(t, vdiskIDA, blockSize, blockCount, false, redisProviderA)
	if storageA == nil {
		t.Fatal("storageA is nil")
	}

	// create storageB, with storageA as its template storage
	memRedisB := redisstub.NewMemoryRedis()
	go memRedisB.Listen()
	defer memRedisB.Close()

	redisProviderB := newTestRedisProvider(memRedisB, memRedisA) // template = memRedisA
	storageB := createTestDedupedStorage(t, vdiskIDB, blockSize, blockCount, true, redisProviderB)
	if storageB == nil {
		t.Fatal("storageB is nil")
	}

	var contentArray [blockCount][]byte

	// store a lot of content in storageA
	for i := int64(0); i < blockCount; i++ {
		contentArray[i] = make([]byte, blockSize)
		rand.Read(contentArray[i])
		err = storageA.Set(i, contentArray[i])
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
		content, err := storageB.Get(i)
		if err != nil {
			t.Fatal(i, err)
		}
		if bytes.Compare(contentArray[i], content) != 0 {
			t.Fatal(i, "unexpected content")
		}
	}
}

func TestDedupedBackendReadWrite(t *testing.T) {
	const (
		vdiskID    = "a"
		size       = 64
		blockSize  = 8
		blockCount = size / blockSize
	)

	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	provider := newTestRedisProvider(memRedis, nil) // template = nil

	ctx := context.Background()

	testBackendReadWrite(ctx, t, vdiskID, blockSize, size,
		createTestDedupedStorage(t, vdiskID, blockSize, blockCount, false, provider))
}

func init() {
	log.SetLevel(log.DebugLevel)
}
