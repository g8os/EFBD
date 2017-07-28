package storage

import (
	"bytes"
	"crypto/rand"
	"runtime/debug"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/redisstub"
)

// testNondedupContentExists tests if
// the given content exists in the database
func testNondedupContentExists(t *testing.T, provider ardb.DataConnProvider, vdiskID string, blockIndex int64, content []byte) {
	conn, err := provider.DataConnection(0)
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	storageKey := nonDedupedStorageKey(vdiskID)
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
func testNondedupContentDoesNotExist(t *testing.T, provider ardb.DataConnProvider, vdiskID string, blockIndex int64, content []byte) {
	conn, err := provider.DataConnection(0)
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	storageKey := nonDedupedStorageKey(vdiskID)
	contentReceived, err := redis.Bytes(conn.Do("HGET", storageKey, blockIndex))

	if err != nil || bytes.Compare(content, contentReceived) != 0 {
		return
	}

	debug.PrintStack()
	t.Fatalf(
		"content found (%v), while it shouldn't exist", contentReceived)
}

func TestNondedupedContent(t *testing.T) {
	const (
		vdiskID = "a"
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := NonDeduped(vdiskID, "", 8, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorage(t, storage)
}

func TestNondedupedContentForceFlush(t *testing.T) {
	const (
		vdiskID = "a"
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := NonDeduped(vdiskID, "", 8, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorageForceFlush(t, storage)
}

// test in a response to https://github.com/zero-os/0-Disk/issues/89
func TestNonDedupedDeadlock(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := NonDeduped(vdiskID, "", blockSize, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorageDeadlock(t, blockSize, blockCount, storage)
}

// test if content linked to (copied) data,
// is available only on the template storage,
// while not yet on the primary storage
// supported since: https://github.com/zero-os/0-Disk/issues/223
func TestGetNondedupedTemplateContent(t *testing.T) {
	const (
		vdiskID = "a"
	)

	redisProviderA := redisstub.NewInMemoryRedisProvider(nil)
	storageA, err := NonDeduped(vdiskID, "", 8, true, redisProviderA)
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	redisProviderB := redisstub.NewInMemoryRedisProvider(redisProviderA)
	storageB, err := NonDeduped(vdiskID, "", 8, true, redisProviderB)
	if err != nil || storageB == nil {
		t.Fatalf("storageB could not be created: %v", err)
	}

	testContent := []byte{4, 2}

	var testBlockIndex int64 // 0

	// content shouldn't exist in either of the 2 volumes
	testNondedupContentDoesNotExist(t, redisProviderA, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, redisProviderB, vdiskID, testBlockIndex, testContent)

	// store content in storageA
	err = storageA.SetBlock(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageA, but not yet in storageB
	testNondedupContentExists(t, redisProviderA, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, redisProviderB, vdiskID, testBlockIndex, testContent)

	// getting content from storageA should be possible
	content, err := storageA.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist, while received nil-content")
	}

	// getting the content now should work
	content, err = storageB.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// content should now be in both storages
	// as the template content should also have been stored in primary storage
	testNondedupContentExists(t, redisProviderA, vdiskID, testBlockIndex, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testNondedupContentExists(t, redisProviderB, vdiskID, testBlockIndex, testContent)

	// let's store some new content in storageB
	testContent = []byte{9, 2}
	testBlockIndex++

	err = storageB.SetBlock(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageB, but not yet in storageA
	testNondedupContentExists(t, redisProviderB, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, redisProviderA, vdiskID, testBlockIndex, testContent)

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
	testNondedupContentExists(t, redisProviderB, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, redisProviderA, vdiskID, testBlockIndex, testContent)

	// if we now make sure storageA, has storageB as its template,
	// our previous Get attempt /will/ work
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
	testNondedupContentExists(t, redisProviderB, vdiskID, testBlockIndex, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testNondedupContentExists(t, redisProviderA, vdiskID, testBlockIndex, testContent)
}

func TestGetNondedupedTemplateContentDeadlock(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 256
	)

	var (
		err error
	)

	redisProviderA := redisstub.NewInMemoryRedisProvider(nil)
	storageA, err := NonDeduped(vdiskID, "", blockSize, false, redisProviderA)
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	redisProviderB := redisstub.NewInMemoryRedisProvider(redisProviderA)
	storageB, err := NonDeduped(vdiskID, "", blockSize, true, redisProviderB)
	if err != nil || storageB == nil {
		t.Fatalf("storageB could not be created: %v", err)
	}

	var contentArray [blockCount][]byte

	// store a lot of content in storageA
	for i := int64(0); i < blockCount; i++ {
		contentArray[i] = make([]byte, blockSize)
		rand.Read(contentArray[i])
		err = storageA.SetBlock(i, contentArray[i])
		if err != nil {
			t.Fatal(i, err)
		}
	}

	// now restore all content
	// which will test the deadlock of async restoring
	for i := int64(0); i < blockCount; i++ {
		content, err := storageB.GetBlock(i)
		if err != nil {
			t.Fatal(i, err)
		}
		if bytes.Compare(contentArray[i], content) != 0 {
			t.Fatal(i, "unexpected content", contentArray[i], content)
		}
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
