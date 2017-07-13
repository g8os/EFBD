package ardb

import (
	"bytes"
	"crypto/rand"
	"runtime/debug"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/redisstub"
)

func createTestNondedupedStorage(t *testing.T, vdiskID string, blockSize int64, templateSupport bool, provider *testRedisProvider) *nonDedupedStorage {
	return newNonDedupedStorage(vdiskID, vdiskID, blockSize, templateSupport, provider).(*nonDedupedStorage)
}

// testNondedupContentExists tests if
// the given content exists in the database
func testNondedupContentExists(t *testing.T, memRedis *redisstub.MemoryRedis, vdiskID string, blockIndex int64, content []byte) {
	conn, err := memRedis.Dial("", 0)
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	storageKey := NonDedupedStorageKey(vdiskID)
	contentReceived, err := redis.Bytes(conn.Do("HGET", storageKey, blockIndex))
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

// testNondedupContentDoesNotExist tests if
// the given content does not exist in the database
func testNondedupContentDoesNotExist(t *testing.T, memRedis *redisstub.MemoryRedis, vdiskID string, blockIndex int64, content []byte) {
	conn, err := memRedis.Dial("", 0)
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	storageKey := NonDedupedStorageKey(vdiskID)
	contentReceived, err := redis.Bytes(conn.Do("HGET", storageKey, blockIndex))

	if err != nil || bytes.Compare(content, contentReceived) != 0 {
		return
	}

	debug.PrintStack()
	t.Fatalf(
		"content found (%v), while it shouldn't exist", contentReceived)
}

func TestNondedupedContent(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, 8, false, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorage(t, storage)
}

func TestNondedupedContentForceFlush(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID = "a"
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, 8, false, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorageForceFlush(t, storage)
}

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestNonDedupedDeadlock(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, blockSize, false, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	testBackendStorageDeadlock(t, blockSize, blockCount, storage)
}

// test if content linked to local (copied) data,
// is available only on the remote (root) storage,
// while not yet on the local storage
// supported since: https://github.com/zero-os/0-Disk/issues/223
func TestGetNondedupedRootContent(t *testing.T) {
	// create storageA
	memRedisA := redisstub.NewMemoryRedis()
	go memRedisA.Listen()
	defer memRedisA.Close()

	const (
		vdiskID = "a"
	)

	redisProviderA := newTestRedisProvider(memRedisA, nil) // root = nil, later will be non-nil
	storageA := createTestNondedupedStorage(t, vdiskID, 8, true, redisProviderA)
	if storageA == nil {
		t.Fatal("storageA is nil")
	}

	// create storageB, with storageA as its fallback/root
	memRedisB := redisstub.NewMemoryRedis()
	go memRedisB.Listen()
	defer memRedisB.Close()

	redisProviderB := newTestRedisProvider(memRedisB, memRedisA) // root = memRedisA
	storageB := createTestNondedupedStorage(t, vdiskID, 8, true, redisProviderB)
	if storageB == nil {
		t.Fatal("storageB is nil")
	}

	testContent := []byte{4, 2}

	var testBlockIndex int64 // 0

	// content shouldn't exist in either of the 2 volumes
	testNondedupContentDoesNotExist(t, memRedisA, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, memRedisB, vdiskID, testBlockIndex, testContent)

	// store content in storageA
	err := storageA.Set(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageA, but not yet in storageB
	testNondedupContentExists(t, memRedisA, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, memRedisB, vdiskID, testBlockIndex, testContent)

	// getting content from storageA should be possible
	content, err := storageA.Get(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist, while received nil-content")
	}

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
	testNondedupContentExists(t, memRedisA, vdiskID, testBlockIndex, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testNondedupContentExists(t, memRedisB, vdiskID, testBlockIndex, testContent)

	// let's store some new content in storageB
	testContent = []byte{9, 2}
	testBlockIndex++

	err = storageB.Set(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageB, but not yet in storageA
	testNondedupContentExists(t, memRedisB, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, memRedisA, vdiskID, testBlockIndex, testContent)

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
	testNondedupContentExists(t, memRedisB, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, memRedisA, vdiskID, testBlockIndex, testContent)

	// if we now make sure storageA, has storageB as its root,
	// our previous Get attempt /will/ work
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
	testNondedupContentExists(t, memRedisB, vdiskID, testBlockIndex, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testNondedupContentExists(t, memRedisA, vdiskID, testBlockIndex, testContent)
}

func TestGetNondedupedRootContentDeadlock(t *testing.T) {
	// create storageA
	memRedisA := redisstub.NewMemoryRedis()
	go memRedisA.Listen()
	defer memRedisA.Close()

	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 256
	)

	var (
		err error
	)

	redisProviderA := newTestRedisProvider(memRedisA, nil) // root = nil, later will be non-nil
	storageA := createTestNondedupedStorage(t, vdiskID, blockSize, false, redisProviderA)
	if storageA == nil {
		t.Fatal("storageA is nil")
	}

	// create storageB, with storageA as its fallback/root
	memRedisB := redisstub.NewMemoryRedis()
	go memRedisB.Listen()
	defer memRedisB.Close()

	redisProviderB := newTestRedisProvider(memRedisB, memRedisA) // root = memRedisA
	storageB := createTestNondedupedStorage(t, vdiskID, blockSize, true, redisProviderB)
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

func init() {
	log.SetLevel(log.DebugLevel)
}
