package storage

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/redisstub"
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

			preContent := make([]byte, blockSize)
			rand.Read(preContent)

			go func(blockIndex int64) {
				defer wg.Done()

				// set content
				if err := storage.SetBlock(blockIndex, preContent); err != nil {
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
				if bytes.Compare(preContent, postContent) != 0 {
					t.Fatal(time, blockIndex, "unexpected content received", preContent, postContent)
				}
			}(i)
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

func TestPipelineErrors(t *testing.T) {
	assert := assert.New(t)

	var errs pipelineErrors
	assert.Nil(errs, "should be nil by default")

	errs.AddError(nil)
	errs.AddErrorMsg(nil, "foo")
	assert.Nil(errs, "should still be nil as no errors were added")

	errs.AddError(errors.New("foo"))
	assert.NotNil(errs, "should not be nil any longer")
	assert.Equal("foo", errs.Error())

	errs = nil
	assert.Nil(errs, "should be nil once again")

	errs.AddErrorMsg(errors.New("foo"), "bar")
	assert.NotNil(errs, "should not be nil any longer")
	assert.Equal("bar (foo)", errs.Error())

	errs = nil
	assert.Nil(errs, "should be nil once again")

	errs.AddErrorMsg(errors.New("foo"), "")
	assert.NotNil(errs, "should not be nil any longer")
	assert.Equal("foo", errs.Error())
}

type testStorageOp struct {
	cmd       string
	args      []interface{}
	validator func(interface{}, error) error
}

func (op *testStorageOp) Send(sender storageOpSender) error {
	return sender.Send(op.cmd, op.args...)
}

func (op *testStorageOp) Receive(receiver storageOpReceiver) error {
	reply, err := receiver.Receive()

	if op.validator != nil {
		return op.validator(reply, err)
	}

	return err
}

func (op *testStorageOp) Label() string {
	return "test storage op"
}

func testStorageSetOp(key string, value interface{}) storageOp {
	return &testStorageOp{
		cmd:  "SET",
		args: []interface{}{key, value},
	}
}

func testStorageDeleteOp(key string) storageOp {
	return &testStorageOp{
		cmd:  "DEL",
		args: []interface{}{key},
		validator: func(reply interface{}, err error) error {
			deleted, err := ardb.Bool(reply, err)
			if err != nil {
				return err
			}

			if !deleted {
				return fmt.Errorf("%v was not deleted, as it didn't exist", key)
			}

			return nil
		},
	}
}

func testStorageGetOp(key string, expected interface{}) storageOp {
	return &testStorageOp{
		cmd:  "GET",
		args: []interface{}{key},
		validator: func(reply interface{}, err error) error {
			if err != nil {
				return err
			}

			var value interface{}

			switch expected.(type) {
			case int:
				value, err = ardb.Int(reply, err)
			case string:
				value, err = ardb.String(reply, err)
			}

			if err != nil {
				return err
			}

			if value != expected {
				return fmt.Errorf("received %v, expected %v", value, expected)
			}

			return nil
		},
	}
}

func TestStorageOpPipeline(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	defer memRedis.Close()
	cfg := memRedis.StorageServerConfig()

	var pipeline storageOpPipeline

	assert := assert.New(t)
	assert.Nil(pipeline, "should still be nil")

	pipeline.Clear()
	assert.Nil(pipeline, "should still be nil")

	pipeline.Add(nil)
	assert.Nil(pipeline, "should still be nil, as adding nil ops shouldn't work")

	err := pipeline.Apply(cfg)
	assert.NoError(err,
		"should never result in an error, as there was nothing to apply")

	pipeline.Add(testStorageGetOp("foo", "bar"))
	assert.Equal(1, len(pipeline))
	err = pipeline.Apply(cfg)
	assert.Error(err, "getting non-existent stuff should not be fine")

	pipeline.Clear()
	assert.Nil(pipeline, "should be nil again")
	assert.Equal(0, len(pipeline))

	pipeline.Add(testStorageSetOp("foo", "bar"))
	assert.NotNil(pipeline, "should not be nil anymore")
	assert.Equal(1, len(pipeline))
	pipeline.Add(testStorageSetOp("answer", 42))
	assert.NotNil(pipeline, "should not be nil anymore")
	assert.Equal(2, len(pipeline))

	err = pipeline.Apply(cfg)
	assert.NoError(err, "setting stuff should work")

	pipeline.Clear()
	assert.Nil(pipeline, "should be nil again")
	assert.Equal(0, len(pipeline))

	pipeline.Add(testStorageGetOp("foo", "bar"))
	assert.Equal(1, len(pipeline))
	pipeline.Add(testStorageDeleteOp("foo"))
	assert.Equal(2, len(pipeline))
	pipeline.Add(testStorageGetOp("answer", 42))
	assert.Equal(3, len(pipeline))
	err = pipeline.Apply(cfg)
	assert.NoError(err, "getting and deleting existent stuff should be fine")

	pipeline.Clear()
	assert.Nil(pipeline, "should be nil again")
	assert.Equal(0, len(pipeline))

	pipeline.Add(testStorageGetOp("foo", "bar"))
	assert.Equal(1, len(pipeline))
	err = pipeline.Apply(cfg)
	assert.Error(err, "getting non-existent existent stuff shouldn't be fine")

	pipeline.Clear()
	assert.Nil(pipeline, "should be nil again")
	assert.Equal(0, len(pipeline))

	pipeline.Add(testStorageGetOp("answer", 42))
	assert.Equal(1, len(pipeline))
	err = pipeline.Apply(cfg)
	assert.NoError(err, "getting existent stuff should be fine")
}

func init() {
	log.SetLevel(log.DebugLevel)
}
