package ardb

import (
	"bytes"
	"errors"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/g8os/blockstor/nbdserver/lba"
	"github.com/g8os/blockstor/redisstub"
	"github.com/garyburd/redigo/redis"
)

type testRedisProvider struct {
	memRedis     *redisstub.MemoryRedis
	rootMemRedis *redisstub.MemoryRedis
}

func (trp *testRedisProvider) RedisConnection(index int64) (redis.Conn, error) {
	return trp.memRedis.Dial("")
}

func (trp *testRedisProvider) FallbackRedisConnection(index int64) (redis.Conn, error) {
	if trp.rootMemRedis == nil {
		return nil, errors.New("no root memredis available")
	}

	return trp.rootMemRedis.Dial("")
}

func (trp *testRedisProvider) MetaRedisConnection() (redis.Conn, error) {
	return trp.memRedis.Dial("")
}

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

// testContentExists tests if
// the given content exists in the database
func testContentExists(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte) {
	conn, err := memRedis.Dial("")
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := lba.HashBytes(content)

	contentReceived, err := redis.Bytes(conn.Do("GET", hash))
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

// testContentDoesNotExist tests if
// the given content does not exist in the database
func testContentDoesNotExist(t *testing.T, memRedis *redisstub.MemoryRedis, content []byte) {
	conn, err := memRedis.Dial("")
	if err != nil {
		debug.PrintStack()
		t.Fatal(err)
	}
	defer conn.Close()

	hash := lba.HashBytes(content)

	exists, err := redis.Bool(conn.Do("EXISTS", hash))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf(
			"content found (%v), while it shouldn't exist", content)
	}
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

	redisProviderA := &testRedisProvider{memRedisA, nil} // root = nil
	storageA := createTestDedupedStorage(t, vdiskIDA, 8, 8, redisProviderA)
	if storageA == nil {
		t.Fatal("storageA is nil")
	}

	// create storageB, with storageA as its fallback/root
	memRedisB := redisstub.NewMemoryRedis()
	go memRedisB.Listen()
	defer memRedisB.Close()

	redisProviderB := &testRedisProvider{memRedisB, memRedisA} // root = memRedisA
	storageB := createTestDedupedStorage(t, vdiskIDB, 8, 8, redisProviderB)
	if storageB == nil {
		t.Fatal("storageB is nil")
	}

	testContent := []byte{4, 2}

	// content shouldn't exist in either of the 2 volumes
	testContentDoesNotExist(t, memRedisA, testContent)
	testContentDoesNotExist(t, memRedisB, testContent)

	var testBlockIndex int64 // 0

	// store content in storageA
	err := storageA.Set(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageA, but not yet in storageB
	testContentExists(t, memRedisA, testContent)
	testContentDoesNotExist(t, memRedisB, testContent)

	// let's flush to ensure metadata is written to external storage
	err = storageA.Flush()
	if err != nil {
		t.Fatal(err)
	}
	// give database some time to process the flush
	time.Sleep(time.Millisecond * 100)

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
	testContentExists(t, memRedisA, testContent)
	testContentDoesNotExist(t, memRedisB, testContent)

	// flush metadata of storageB first,
	// so it's reloaded next time fresh from externalStorage,
	// this shows that copying metadata to a vdisk which is active,
	// is only going lead to dissapointment
	err = storageB.Flush()
	if err != nil {
		t.Fatal(err)
	}
	// give database some time to process the flush
	time.Sleep(time.Millisecond * 100)

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
	// give database some time to process the async storing
	time.Sleep(time.Millisecond * 100)

	// content should now be in both storages
	// as the remote get should have also stored the content locally
	testContentExists(t, memRedisA, testContent)
	testContentExists(t, memRedisB, testContent)

	// let's store some new content in storageB
	testContent = []byte{9, 2}
	testBlockIndex++

	err = storageB.Set(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageB, but not yet in storageA
	testContentExists(t, memRedisB, testContent)
	testContentDoesNotExist(t, memRedisA, testContent)

	// let's flush to ensure metadata is written to external storage
	err = storageB.Flush()
	if err != nil {
		t.Fatal(err)
	}
	// give database some time to process the flush
	time.Sleep(time.Millisecond * 100)

	// flush metadata of storageA first,
	// so it's reloaded next time fresh from externalStorage,
	// this shows that copying metadata to a vdisk which is active,
	// is only going lead to dissapointment
	err = storageA.Flush()
	if err != nil {
		t.Fatal(err)
	}
	// give database some time to process the flush
	time.Sleep(time.Millisecond * 100)

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
	testContentExists(t, memRedisB, testContent)
	testContentDoesNotExist(t, memRedisA, testContent)

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
	// give database some time to process the async storing
	time.Sleep(time.Millisecond * 100)

	// and also our direct test should show that
	// the content now exists in both storages
	testContentExists(t, memRedisB, testContent)
	testContentExists(t, memRedisA, testContent)

}
