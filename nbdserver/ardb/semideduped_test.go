package ardb

import (
	"bytes"
	"testing"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbdserver/lba"
	"github.com/zero-os/0-Disk/redisstub"
)

func createTestSemiDedupedStorage(t *testing.T, vdiskID string, blockSize, blockCount int64, provider *testRedisProvider) backendStorage {
	lba, err := lba.NewLBA(
		vdiskID,
		blockCount,
		DefaultLBACacheLimit,
		provider)
	if err != nil {
		t.Fatal("couldn't create LBA", err)
	}

	return newSemiDedupedStorage(vdiskID, blockSize, provider, lba)
}

func TestSemiDedupedContentBasic(t *testing.T) {
	const (
		templateIndexA = 0
		templateIndexB = 1

		blockSize  = 8
		blockCount = 8
	)
	var (
		templateContentA = []byte{1, 2, 3, 4, 5, 6, 7, 8}
		templateContentB = []byte{8, 7, 6, 5, 4, 3, 2, 1}
	)
	templateRedis := func() *redisstub.MemoryRedis {
		memRedis := redisstub.NewMemoryRedis()
		go func() {
			defer memRedis.Close()
			memRedis.Listen()
		}()

		redisProvider := newTestRedisProvider(memRedis, nil) // template = nil
		template := createTestDedupedStorage(t, "template", blockSize, blockCount, false, redisProvider)
		if template == nil {
			t.Fatal("template is nil")
		}

		err := template.Set(templateIndexA, templateContentA[:])
		if err != nil {
			t.Fatalf("setting templateIndexA failed: %v", err)
		}

		err = template.Set(templateIndexB, templateContentB[:])
		if err != nil {
			t.Fatalf("setting templateIndexB failed: %v", err)
		}

		err = template.Flush()
		if err != nil {
			t.Fatalf("flushing template failed: %v", err)
		}

		return memRedis
	}()
	if templateRedis == nil {
		t.Fatal("templateRedis is nil")
	}

	memRedis := redisstub.NewMemoryRedis()
	go func() {
		defer memRedis.Close()
		memRedis.Listen()
	}()
	redisProvider := newTestRedisProvider(memRedis, templateRedis)

	copyTestMetaData(t, "template", "a",
		newTestRedisProvider(templateRedis, nil),
		redisProvider)

	storage := createTestSemiDedupedStorage(t, "a", blockSize, blockCount, redisProvider)
	if storage == nil {
		t.Fatal("storage is nil")
	}

	const (
		userIndexA = 3
	)
	var (
		userContentA = []byte{4, 2}
	)

	// set some user content
	err := storage.Set(userIndexA, userContentA)
	if err != nil {
		t.Fatalf("setting userIndexA failed: %v", err)
	}
	// get that user content
	content, err := storage.Get(userIndexA)
	if err != nil {
		t.Fatalf("getting userIndexA failed: %v", err)
	}
	if len(content) < 2 || bytes.Compare(userContentA, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// try to get template content A
	content, err = storage.Get(templateIndexA)
	if err != nil {
		t.Fatalf("getting templateIndexA failed: %v", err)
	}
	if bytes.Compare(templateContentA, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// try to get template content B
	content, err = storage.Get(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if bytes.Compare(templateContentB, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// overwrite template content B
	err = storage.Set(templateIndexB, userContentA)
	if err != nil {
		t.Fatalf("setting templateIndexB failed: %v", err)
	}
	// and ensure that content is indeed set
	content, err = storage.Get(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if len(content) < 2 || bytes.Compare(userContentA, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// delete content
	err = storage.Delete(templateIndexB)
	if err != nil {
		t.Fatalf("deleting templateIndexB failed: %v", err)
	}
	// and ensure that content is indeed deleted
	content, err = storage.Get(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if content != nil {
		t.Fatalf("unexpected content found: %v", content)
	}

	// let's merge template with user content
	err = storage.Merge(templateIndexA, 1, userContentA)
	if err != nil {
		t.Fatalf("merging templateIndexA failed: %v", err)
	}
	// let's check if the merging went fine
	content, err = storage.Get(templateIndexA)
	if err != nil {
		t.Fatalf("getting templateIndexA failed: %v", err)
	}
	if bytes.Compare([]byte{1, 4, 2, 4, 5, 6, 7, 8}, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// let's merge nil content with user content
	err = storage.Merge(templateIndexB, 2, userContentA)
	if err != nil {
		t.Fatalf("merging templateIndexB failed: %v", err)
	}
	// let's check if the merging went fine
	content, err = storage.Get(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if bytes.Compare([]byte{0, 0, 4, 2, 0, 0, 0, 0}, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
