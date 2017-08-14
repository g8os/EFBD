package storage

import (
	"bytes"
	"crypto/rand"
	"runtime/debug"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
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

/*
func TestNonDedupedVdiskExists(t *testing.T) {
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

	storage, err := NonDeduped(vdiskID, "", 8, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	exists, err := NonDedupedVdiskExists(vdiskID, &clusterConfig)
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

	exists, err = NonDedupedVdiskExists(vdiskID, &clusterConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

	exists, err = NonDedupedVdiskExists(vdiskID, &clusterConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Fatal("vdisk shouldn't exist anymore, but it does")
	}
}*/

func TestListNonDedupedBlockIndices(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 9
	)

	redisProvider := redisstub.NewInMemoryRedisProviderMultiServers(4, false)
	defer redisProvider.Close()
	clusterConfig := redisProvider.ClusterConfig()

	storage, err := NonDeduped(vdiskID, "", blockSize, false, redisProvider)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	indices, err := ListNonDedupedBlockIndices(vdiskID, clusterConfig)
	if err == nil {
		t.Fatalf("expected an error, as no indices exist yet: %v", indices)
	}

	var expectedIndices []int64

	// store all blocks, and make sure the List returns all indices correctly
	for i := 0; i < blockCount; i++ {
		// store a block
		data := make([]byte, blockSize)
		_, err := rand.Read(data)
		if err != nil {
			t.Fatalf("couldn't generate a random block (%d): %v", i, err)
		}
		err = storage.SetBlock(int64(i), data)
		if err != nil {
			t.Fatalf("couldn't set block (%d): %v", i, err)
		}

		err = storage.Flush()
		if err != nil {
			t.Fatalf("couldn't flush storage (step %d): %v", i, err)
		}

		// now test if listing the indices is correct
		indices, err := ListNonDedupedBlockIndices(vdiskID, clusterConfig)
		if err != nil {
			t.Fatalf("couldn't list deduped block indices (step %d): %v", i, err)
		}

		expectedIndices = append(expectedIndices, int64(i))
		if assert.Len(t, indices, i+1) {
			assert.Equal(t, expectedIndices, indices)
		}
	}

	// delete all odd blocks
	ci := 1
	for i := 1; i < blockCount; i += 2 {
		err = storage.DeleteBlock(int64(i))
		if err != nil {
			t.Fatalf("couldn't delete block %d: %v", i, err)
		}

		expectedIndices = append(expectedIndices[:ci], expectedIndices[ci+1:]...)

		// now test if listing the indices is still correct
		indices, err := ListNonDedupedBlockIndices(vdiskID, clusterConfig)
		if err != nil {
			t.Fatalf("couldn't list deduped block indices (step %d): %v", i, err)
		}

		if assert.Len(t, indices, len(expectedIndices), "at cut index %v", i) {
			assert.Equal(t, expectedIndices, indices, "at cut index %v", i)
		}

		ci++
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
