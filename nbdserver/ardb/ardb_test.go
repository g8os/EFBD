package ardb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/g8os/blockstor/redisstub"
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
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

// shared test function to test all types of backendStorage equally,
// this gives us some confidence that all storages behave the same
// from an end-user perspective
func testBackendStorage(t *testing.T, storage backendStorage) {
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

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LDebug)
}
