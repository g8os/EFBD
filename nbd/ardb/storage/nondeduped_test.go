package storage

import (
	"bytes"
	"crypto/rand"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/redisstub"
)

// testNondedupContentExists tests if
// the given content exists in the database
func testNondedupContentExists(t *testing.T, cluster ardb.StorageCluster, vdiskID string, blockIndex int64, content []byte) {
	storageKey := nonDedupedStorageKey(vdiskID)
	contentReceived, err := ardb.Bytes(
		cluster.DoFor(blockIndex, ardb.Command(command.HashGet, storageKey, blockIndex)))
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
func testNondedupContentDoesNotExist(t *testing.T, cluster ardb.StorageCluster, vdiskID string, blockIndex int64, content []byte) {
	storageKey := nonDedupedStorageKey(vdiskID)
	contentReceived, err := ardb.Bytes(
		cluster.DoFor(blockIndex, ardb.Command(command.HashGet, storageKey, blockIndex)))

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

	cluster := redisstub.NewUniCluster(false)
	defer cluster.Close()

	storage, err := NonDeduped(vdiskID, "", 8, cluster, nil)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	testBlockStorage(t, storage)
}

func TestNondedupedContentForceFlush(t *testing.T) {
	const (
		vdiskID = "a"
	)

	cluster := redisstub.NewUniCluster(false)
	defer cluster.Close()

	storage, err := NonDeduped(vdiskID, "", 8, cluster, nil)
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

	cluster := redisstub.NewUniCluster(true)
	defer cluster.Close()

	storage, err := NonDeduped(vdiskID, "", blockSize, cluster, nil)
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

	clusterA := redisstub.NewUniCluster(false)
	defer clusterA.Close()

	storageA, err := NonDeduped(vdiskID, "", 8, clusterA, ardb.ErrorCluster{Error: ardb.ErrNoServersAvailable})
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	clusterB := redisstub.NewUniCluster(false)
	defer clusterB.Close()

	storageB, err := NonDeduped(vdiskID, "", 8, clusterB, clusterA)
	if err != nil || storageB == nil {
		t.Fatalf("storageB could not be created: %v", err)
	}

	testContent := []byte{4, 2}

	var testBlockIndex int64 // 0

	// content shouldn't exist in either of the 2 volumes
	testNondedupContentDoesNotExist(t, clusterA, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, clusterB, vdiskID, testBlockIndex, testContent)

	// store content in storageA
	err = storageA.SetBlock(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageA, but not yet in storageB
	testNondedupContentExists(t, clusterA, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, clusterB, vdiskID, testBlockIndex, testContent)

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
	testNondedupContentExists(t, clusterA, vdiskID, testBlockIndex, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testNondedupContentExists(t, clusterB, vdiskID, testBlockIndex, testContent)

	// let's store some new content in storageB
	testContent = []byte{9, 2}
	testBlockIndex++

	err = storageB.SetBlock(testBlockIndex, testContent)
	if err != nil {
		t.Fatal(err)
	}
	// content should now exist in storageB, but not yet in storageA
	testNondedupContentExists(t, clusterB, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, clusterA, vdiskID, testBlockIndex, testContent)

	// let's now try to get it from storageA
	// this should fail (manifested as nil-content), as storageA has no template,
	// and the content isn't available in primary storage
	content, err = storageA.GetBlock(testBlockIndex)
	if err != nil && err != ardb.ErrNoServersAvailable {
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
	testNondedupContentExists(t, clusterB, vdiskID, testBlockIndex, testContent)
	testNondedupContentDoesNotExist(t, clusterA, vdiskID, testBlockIndex, testContent)

	// if we now make sure storageA, has storageB as its template,
	// our previous Get attempt /will/ work
	storageA.(*nonDedupedStorage).templateCluster = clusterB

	content, err = storageA.GetBlock(testBlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if content == nil {
		t.Fatal("content should exist now, while received nil-content")
	}

	// and also our direct test should show that
	// the content now exists in both storages
	testNondedupContentExists(t, clusterB, vdiskID, testBlockIndex, testContent)

	// wait until the Get method saves the content async
	time.Sleep(time.Millisecond * 200)
	testNondedupContentExists(t, clusterA, vdiskID, testBlockIndex, testContent)
}

// test feature implemented for
// https://github.com/zero-os/0-Disk/issues/369
func TestNonDedupedStorageTemplateServerDown(t *testing.T) {
	const (
		vdiskID = "a"
	)

	clusterA := redisstub.NewUniCluster(false)
	defer clusterA.Close()

	storageA, err := NonDeduped(vdiskID, "", 8, clusterA, nil)
	if err != nil || storageA == nil {
		t.Fatalf("storageA could not be created: %v", err)
	}

	clusterB := redisstub.NewUniCluster(false)
	defer clusterB.Close()

	storageB, err := NonDeduped(vdiskID, "", 8, clusterB, clusterA)
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

	// now get that content in storageB, should be possible
	content, err := storageB.GetBlock(someIndex)
	if err != nil {
		t.Fatalf("could not read template content at index %d: %v", someIndex, err)
	}
	if !bytes.Equal(someContent, content) {
		t.Fatalf("invalid content for block index %d: %v", someIndex, content)
	}

	// now mark template invalid, and that should make it return an expected error instead
	storageB.(*nonDedupedStorage).templateCluster = ardb.ErrorCluster{Error: ardb.ErrNoServersAvailable}
	content, err = storageB.GetBlock(someIndexPlusOne)
	if len(content) != 0 {
		t.Fatalf("content should be empty but was was: %v",
			content)
	}
	if err != ardb.ErrNoServersAvailable {
		t.Fatalf("error should be '%v', but instead was: %v",
			ardb.ErrNoServersAvailable, err)
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

// This test tests both if the amount of block indices listed for a nondeduped vdisk is correct,
// as well as the fact that all block indices are always in sorted order from small to big.
func TestListNonDedupedBlockIndices(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 16
	)

	cluster := redisstub.NewCluster(4, false)
	defer cluster.Close()

	storage, err := NonDeduped(vdiskID, "", blockSize, cluster, nil)
	if err != nil || storage == nil {
		t.Fatalf("storage could not be created: %v", err)
	}

	indices, err := listNonDedupedBlockIndices(vdiskID, cluster)
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
	if len(indices) > 0 {
		t.Fatalf("expexted no indices: %v", indices)
	}

	var expectedIndices []int64

	// store all blocks, and make sure the List returns all indices correctly
	for i := 0; i < blockCount; i++ {
		blockIndex := int64(i * i)

		// store a block
		data := make([]byte, blockSize)
		_, err := rand.Read(data)
		if err != nil {
			t.Fatalf("couldn't generate a random block (%d): %v", i, err)
		}
		err = storage.SetBlock(blockIndex, data)
		if err != nil {
			t.Fatalf("couldn't set block (%d): %v", blockIndex, err)
		}

		err = storage.Flush()
		if err != nil {
			t.Fatalf("couldn't flush storage (step %d): %v", blockIndex, err)
		}

		fetchedData, err := storage.GetBlock(blockIndex)
		if err != nil {
			t.Fatalf("couldn't get block (%d): %v", blockIndex, err)
		}
		if !assert.Equal(t, data, fetchedData) {
			t.Fatalf("couldn't get correct block %d", blockIndex)
		}

		// now test if listing the indices is correct
		indices, err := listNonDedupedBlockIndices(vdiskID, cluster)
		if err != nil {
			t.Fatalf("couldn't list non-deduped block indices (step %d): %v", i, err)
		}

		expectedIndices = append(expectedIndices, blockIndex)
		if assert.Len(t, indices, len(expectedIndices)) {
			assert.Equal(t, expectedIndices, indices)
		}
	}

	// delete all odd blocks
	ci := 1
	for i := 1; i < blockCount; i += 2 {
		blockIndex := int64(i * i)

		err = storage.DeleteBlock(blockIndex)
		if err != nil {
			t.Fatalf("couldn't delete block %d: %v", blockIndex, err)
		}

		err = storage.Flush()
		if err != nil {
			t.Fatalf("couldn't flush storage (step %d): %v", blockIndex, err)
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
		indices, err := listNonDedupedBlockIndices(vdiskID, cluster)
		if err != nil {
			t.Fatalf("couldn't list non-deduped block indices (step %d): %v", i, err)
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
