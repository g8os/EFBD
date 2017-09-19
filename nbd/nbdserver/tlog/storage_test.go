package tlog

import (
	"bytes"
	"testing"

	"github.com/zero-os/0-Disk/nbd/ardb/storage"
)

// shared test function to test all types of BlockStorage equally,
// this gives us some confidence that all storages behave the same
// from an end-user perspective
func testBlockStorage(t *testing.T, storage storage.BlockStorage) {
	var (
		testContentA = []byte{4, 2}
	)
	const (
		testBlockIndexA = 0
		testBlockIndexB = 1
	)

	defer storage.Close()

	// deleting non-existing block is fine
	// this just results in a noop
	err := storage.DeleteBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}

	// getting non-existing blocks,
	// is not an error, and results in nil-block
	content, err := storage.GetBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found block %v, while expected nil-block", content)
	}

	// setting blocks should be always fine
	for i := 0; i < 3; i++ {
		err = storage.SetBlock(testBlockIndexA, testContentA)
		if err != nil {
			t.Fatal(err)
		}
	}

	// getting this block should now be possible
	content, err = storage.GetBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if len(content) < 2 || bytes.Compare(testContentA, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// deleting and getting non-existent block is still fine
	err = storage.DeleteBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	// getting the block should now fail
	content, err = storage.GetBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// Deleting a block, should really delete it
	err = storage.DeleteBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}

	// block should now be nil
	content, err = storage.GetBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// Deleting content, should really delete it
	err = storage.DeleteBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}

	// content should now be nil
	content, err = storage.GetBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}
}

// shared test function to test all types of BlockStorage equally,
// this gives us some confidence that all storages behave the same
// from an end-user perspective
func testBlockStorageForceFlush(t *testing.T, storage storage.BlockStorage) {
	var (
		testContentA = []byte{4, 2}
	)
	const (
		testBlockIndexA = 0
		testBlockIndexB = 1
	)

	defer storage.Close()

	// deleting non-existing content is fine
	// this just results in a noop
	err := storage.DeleteBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}

	// getting non-existing content,
	// is not an error, and results in nil-content
	content, err := storage.GetBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// setting content, and getting set content should be always fine
	for i := 0; i < 3; i++ {
		err = storage.SetBlock(testBlockIndexA, testContentA)
		if err != nil {
			t.Fatal(i, err)
		}

		// getting this content should be possible
		content, err = storage.GetBlock(testBlockIndexA)
		if err != nil {
			t.Fatal(i, err)
		}
		if len(content) < 2 || bytes.Compare(testContentA, content[:2]) != 0 {
			t.Fatalf("iteration %d: unexpected content found: %v", i, content)
		}

		err = storage.Flush()
		if err != nil {
			t.Fatal(i, err)
		}

		// getting this content should still be possible
		content, err = storage.GetBlock(testBlockIndexA)
		if err != nil {
			t.Fatal(err)
		}
		if len(content) < 2 || bytes.Compare(testContentA, content[:2]) != 0 {
			t.Fatalf("iteration %d: unexpected content found: %v", i, content)
		}
	}

	// deleting and getting non-existent content is still fine
	err = storage.DeleteBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	// getting the content should now fail
	content, err = storage.GetBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.Flush()
	if err != nil {
		t.Fatal(err)
	}
	// getting the content should now still fail
	content, err = storage.GetBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// Deleting content, should really delete it
	err = storage.DeleteBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// content should now be nil
	content, err = storage.GetBlock(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// Deleting content, should really delete it
	err = storage.DeleteBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}

	// content should now be nil
	content, err = storage.GetBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	err = storage.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// content should still be nil
	content, err = storage.GetBlock(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}
}
