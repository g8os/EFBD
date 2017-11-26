package storage

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/log"
)

// shared test function to test all types of BlockStorage equally,
// this gives us some confidence that all storages behave the same
// from an end-user perspective
func testBlockStorage(t *testing.T, storage BlockStorage) {
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
func testBlockStorageForceFlush(t *testing.T, storage BlockStorage) {
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

// shared test function to test all types of BlockStorage equally,
// and make sure they don't get in a deadlock situation, after being used for a while.
// test in a response to https://github.com/zero-os/0-Disk/issues/89
func testBlockStorageDeadlock(t *testing.T, blockSize, blockCount int64, storage BlockStorage) {
	defer storage.Close()

	// store random content eight times
	// each time we do all storage async at once,
	// and wait for them all to be done
	for time := int64(0); time < 8; time++ {
		var wg sync.WaitGroup

		for i := int64(0); i < blockCount; i++ {
			wg.Add(1)

			content := make([]byte, blockSize)
			rand.Read(content)

			go func(blockIndex int64, content []byte) {
				defer wg.Done()

				// set content
				if err := storage.SetBlock(blockIndex, content); err != nil {
					t.Fatal(time, blockIndex, err)
					return
				}

				// get content
				postContent, err := storage.GetBlock(blockIndex)
				if err != nil {
					t.Fatal(time, blockIndex, err)
					return
				}

				// make sure the postContent (GET) equals the preContent (SET)
				if bytes.Compare(content, postContent) != 0 {
					t.Error(time, blockIndex, "unexpected content received", content, postContent)
				}
			}(i, content)
		}

		wg.Wait()

		// let's flush each time
		if err := storage.Flush(); err != nil {
			t.Fatal(time, err)
		}
	}

	var wg sync.WaitGroup

	// delete all content (async)
	for i := int64(0); i < blockCount; i++ {
		blockIndex := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := storage.DeleteBlock(blockIndex); err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			// content should now be gone

			postContent, err := storage.GetBlock(blockIndex)
			if err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			if len(postContent) != 0 {
				t.Errorf("didn't expect to find content for #%d: %v", blockIndex, postContent)
			}
		}()
	}

	wg.Wait()

	// let's flush the deleted content
	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}

	// getting all the content should still fail
	for i := int64(0); i < blockCount; i++ {
		blockIndex := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			postContent, err := storage.GetBlock(blockIndex)
			if err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			if len(postContent) != 0 {
				t.Errorf("didn't expect to find content for #%d: %v", blockIndex, postContent)
			}
		}()
	}

	wg.Wait()
}

func TestSortInt64s(t *testing.T) {
	require := require.New(t)
	testCases := []struct {
		input, expected []int64
	}{
		{[]int64{}, []int64{}},
		{[]int64{1}, []int64{1}},
		{[]int64{1, 2}, []int64{1, 2}},
		{[]int64{2, 1}, []int64{1, 2}},
		{[]int64{5, 4, 3, 2, 1}, []int64{1, 2, 3, 4, 5}},
		{[]int64{3, 1, 2}, []int64{1, 2, 3}},
	}
	for _, testCase := range testCases {
		output := make([]int64, len(testCase.input))
		copy(output, testCase.input)
		sortInt64s(output)
		require.Equalf(testCase.expected, output, "%v", testCase)
	}
}

func TestDedupInt64s(t *testing.T) {
	require := require.New(t)
	testCases := []struct {
		input, expected []int64
	}{
		{nil, nil},
		{[]int64{1}, []int64{1}},
		{[]int64{1, 2}, []int64{1, 2}},
		{[]int64{1, 1}, []int64{1}},
		{[]int64{1, 1, 2, 2}, []int64{1, 2}},
		{[]int64{1, 1, 2, 2, 3}, []int64{1, 2, 3}},
		{[]int64{1, 1, 4, 2, 2, 2, 3}, []int64{1, 4, 2, 3}},
	}
	for _, testCase := range testCases {
		output := dedupInt64s(testCase.input)
		require.Equalf(testCase.expected, output, "%v", testCase)
	}
}

func TestDedupStrings(t *testing.T) {
	require := require.New(t)
	testCases := []struct {
		input, expected []string
	}{
		{nil, nil},
		{[]string{"foo"}, []string{"foo"}},
		{[]string{"foo", "foo"}, []string{"foo"}},
		{[]string{"foo", "bar"}, []string{"foo", "bar"}},
		{[]string{"foo", "foo", "bar", "bar", "bar"}, []string{"foo", "bar"}},
		{[]string{"foo", "foo", "bar", "bar", "bar", "baz"}, []string{"foo", "bar", "baz"}},
		{[]string{"foo", "foo", "baz", "bar", "bar", "bar"}, []string{"foo", "baz", "bar"}},
	}
	for _, testCase := range testCases {
		output := dedupStrings(testCase.input)
		require.Equalf(testCase.expected, output, "%v", testCase)
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
