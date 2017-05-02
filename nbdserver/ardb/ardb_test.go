package ardb

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/g8os/blockstor/redisstub"
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

// use pool in testRedisProvider
// as we want to trigger deadlock in pools

func newTestRedisPool(dial func() (redis.Conn, error)) *redis.Pool {
	return &redis.Pool{
		MaxActive:   10,
		MaxIdle:     10,
		Wait:        true,
		IdleTimeout: 5 * time.Second,
		Dial:        dial,
	}
}

func newTestRedisProvider(main, root *redisstub.MemoryRedis) *testRedisProvider {
	provider := &testRedisProvider{
		memRedis:     main,
		rootMemRedis: root,
	}
	provider.mainPool = newTestRedisPool(func() (redis.Conn, error) {
		if provider.memRedis == nil {
			return nil, errors.New("no memory redis available")
		}

		return provider.memRedis.Dial("")
	})
	provider.rootPool = newTestRedisPool(func() (redis.Conn, error) {
		if provider.rootMemRedis == nil {
			return nil, errors.New("no root memory redis available")
		}

		return provider.rootMemRedis.Dial("")
	})
	return provider
}

type testRedisProvider struct {
	memRedis           *redisstub.MemoryRedis
	rootMemRedis       *redisstub.MemoryRedis
	mainPool, rootPool *redis.Pool
}

func (trp *testRedisProvider) RedisConnection(index int64) (redis.Conn, error) {
	return trp.mainPool.Get(), nil
}

func (trp *testRedisProvider) MetaRedisConnection() (redis.Conn, error) {
	return trp.mainPool.Get(), nil
}

func (trp *testRedisProvider) FallbackRedisConnection(index int64) (redis.Conn, error) {
	return trp.rootPool.Get(), nil
}

// shared test function to test all types of backendStorage equally,
// this gives us some confidence that all storages behave the same
// from an end-user perspective
func testBackendStorage(t *testing.T, storage backendStorage) {
	defer storage.Close()
	go storage.GoBackground(context.Background())

	var (
		testContentA = []byte{4, 2}
		testContentB = []byte{9, 2}
	)
	const (
		testBlockIndexA = 0
		testBlockIndexB = 1
	)

	// deleting non-existing content is fine
	// this just results in a noop
	err := storage.Delete(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}

	// getting non-existing content,
	// is not an error, and results in nil-content
	content, err := storage.Get(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// setting content should be always fine
	for i := 0; i < 3; i++ {
		err = storage.Set(testBlockIndexA, testContentA)
		if err != nil {
			t.Fatal(err)
		}
	}

	// getting this content should now be possible
	content, err = storage.Get(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if len(content) < 2 || bytes.Compare(testContentA, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// deleting and getting non-existent content is still fine
	err = storage.Delete(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	// getting the content should now fail
	content, err = storage.Get(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}

	// Merging new content with non existing content is fine
	err = storage.Merge(testBlockIndexB, 0, testContentB)
	if err != nil {
		t.Fatal(err)
	}
	// getting the content should be fine
	content, err = storage.Get(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if len(content) < 2 || bytes.Compare(testContentB, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// Merging existing content is fine as well
	err = storage.Merge(testBlockIndexB, 1, testContentA)
	if err != nil {
		t.Fatal(err)
	}
	// getting the content should be fine
	content, err = storage.Get(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare([]byte{9, 4, 2, 0, 0, 0, 0, 0}, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// Deleting content, should really delete it
	err = storage.Delete(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}

	// content should now be nil
	content, err = storage.Get(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}
	err = storage.Merge(testBlockIndexA, 0, testContentA)
	if err != nil {
		t.Fatal(err)
	}

	// content should be merged
	content, err = storage.Get(testBlockIndexA)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare([]byte{4, 2, 0, 0, 0, 0, 0, 0}, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// Deleting content, should really delete it
	err = storage.Delete(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}

	// content should now be nil
	content, err = storage.Get(testBlockIndexB)
	if err != nil {
		t.Fatal(err)
	}
	if content != nil {
		t.Fatalf("found content %v, while expected nil-content", content)
	}
}

// shared test function to test all types of backendStorage equally,
// and make sure they don't get in a deadlock situation, after being used for a while.
// test in a response to https://github.com/g8os/blockstor/issues/89
func testBackendStorageDeadlock(t *testing.T, blockSize, blockCount int64, storage backendStorage) {
	var err error

	// close & run its thread in background
	defer storage.Close()
	go storage.GoBackground(context.Background())

	// store random content eight times
	// each time we do all storage async at once,
	// and wait for them all to be done
	for time := int64(0); time < 8; time++ {
		var wg sync.WaitGroup

		for i := int64(0); i < blockCount; i++ {
			blockIndex := i
			wg.Add(1)
			go func() {
				defer wg.Done()

				preContent := make([]byte, blockSize)
				rand.Read(preContent)

				// set content
				err = storage.Set(blockIndex, preContent)
				if err != nil {
					t.Fatal(time, blockIndex, err)
					return
				}

				// get content
				postContent, err := storage.Get(blockIndex)
				if err != nil {
					t.Fatal(time, blockIndex, err)
					return
				}

				// make sure the postContent (GET) equals the preContent (SET)
				if bytes.Compare(preContent, postContent) != 0 {
					t.Error(time, blockIndex, "unexpected content received")
				}
			}()
		}

		wg.Wait()

		// let's flush each time
		err = storage.Flush()
		if err != nil {
			t.Error(time, err)
		}
	}

	var wg sync.WaitGroup

	// merge all content four times (async)
	for time := int64(0); time < 4; time++ {
		for i := int64(0); i < blockCount; i += 2 {
			blockIndex := i
			wg.Add(1)
			go func() {
				defer wg.Done()

				// get preContent
				preContent, err := storage.Get(blockIndex)
				if err != nil {
					t.Fatal(time, blockIndex, err)
					return
				}

				// merge it
				offset := 2 + time
				blockIndex = (blockIndex + 1) % blockCount
				err = storage.Merge(blockIndex, offset, preContent)
				if err != nil {
					t.Fatal(time, blockIndex, err)
				}
			}()
		}
	}

	wg.Wait()

	// delete all content (async)
	for i := int64(0); i < blockCount; i++ {
		blockIndex := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = storage.Delete(blockIndex)
			if err != nil {
				t.Fatal(blockIndex, err)
				return
			}

			// content should now be gone

			postContent, err := storage.Get(blockIndex)
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

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LDebug)
}
