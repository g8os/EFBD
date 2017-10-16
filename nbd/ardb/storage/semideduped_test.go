package storage

import (
	"bytes"
	"testing"

	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/redisstub"
)

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

	templateCluster := redisstub.NewUniCluster(false)
	defer templateCluster.Close()

	func() {
		template, err := Deduped(
			"template", blockCount,
			ardb.DefaultLBACacheLimit, templateCluster, nil)
		if err != nil || template == nil {
			t.Fatalf("template storage could not be created: %v", err)
		}

		err = template.SetBlock(templateIndexA, templateContentA[:])
		if err != nil {
			t.Fatalf("setting templateIndexA failed: %v", err)
		}

		err = template.SetBlock(templateIndexB, templateContentB[:])
		if err != nil {
			t.Fatalf("setting templateIndexB failed: %v", err)
		}

		err = template.Flush()
		if err != nil {
			t.Fatalf("flushing template failed: %v", err)
		}
	}()

	cluster := redisstub.NewUniCluster(false)
	defer cluster.Close()

	copyTestMetaData(t, "template", "a", templateCluster, cluster)

	storage, err := SemiDeduped(
		"a", blockSize,
		ardb.DefaultLBACacheLimit, cluster, templateCluster)
	if err != nil || storage == nil {
		t.Fatalf("creating SemiDedupedStorage failed: %v", err)
	}

	const (
		userIndexA = 3
	)
	var (
		userContentA = []byte{4, 2}
	)

	// set some user content
	err = storage.SetBlock(userIndexA, userContentA)
	if err != nil {
		t.Fatalf("setting userIndexA failed: %v", err)
	}
	// get that user content
	content, err := storage.GetBlock(userIndexA)
	if err != nil {
		t.Fatalf("getting userIndexA failed: %v", err)
	}
	if len(content) < 2 || bytes.Compare(userContentA, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// try to get template content A
	content, err = storage.GetBlock(templateIndexA)
	if err != nil {
		t.Fatalf("getting templateIndexA failed: %v", err)
	}
	if bytes.Compare(templateContentA, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// try to get template content B
	content, err = storage.GetBlock(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if bytes.Compare(templateContentB, content) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// overwrite template content B
	err = storage.SetBlock(templateIndexB, userContentA)
	if err != nil {
		t.Fatalf("setting templateIndexB failed: %v", err)
	}
	// and ensure that content is indeed set
	content, err = storage.GetBlock(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if len(content) < 2 || bytes.Compare(userContentA, content[:2]) != 0 {
		t.Fatalf("unexpected content found: %v", content)
	}

	// delete content
	err = storage.DeleteBlock(templateIndexB)
	if err != nil {
		t.Fatalf("deleting templateIndexB failed: %v", err)
	}
	// and ensure that content is indeed deleted
	content, err = storage.GetBlock(templateIndexB)
	if err != nil {
		t.Fatalf("getting templateIndexB failed: %v", err)
	}
	if content != nil {
		t.Fatalf("unexpected content found: %v", content)
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
