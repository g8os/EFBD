package main

import (
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/g8os/blockstor/tlog/schema"
)

const (
	lastHashPrefix = "last_hash_"
)

// tlog table
type tlogTab struct {
	tlogs     []*schema.TlogBlock
	lastFlush time.Time
	lastHash  []byte
	vdiskID   string
	lock      sync.RWMutex
}

func newTlogTab(vdiskID string) *tlogTab {
	return &tlogTab{
		tlogs:     []*schema.TlogBlock{},
		lastFlush: time.Now(),
		vdiskID:   vdiskID,
	}
}

// check if this vdisk need to be flushed
func (t *tlogTab) needFlush(flushSize, flushTime int, periodic bool) bool {
	if !periodic && len(t.tlogs) < flushSize {
		return false
	}

	if periodic && len(t.tlogs) == 0 {
		return false
	}

	if periodic && int(time.Since(t.lastFlush).Seconds()) < flushTime {
		return false
	}

	return true
}

// pick tlogs to be flushed, if possible
func (t *tlogTab) Pick(flushSize, flushTime int, periodic bool) ([]*schema.TlogBlock, bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// check if we need to flush
	if !t.needFlush(flushSize, flushTime, periodic) {
		return nil, false
	}

	// get pick size
	pickLen := flushSize
	if len(t.tlogs) < pickLen {
		pickLen = len(t.tlogs)
	}

	// get the blocks
	blocks := t.tlogs[:pickLen]

	t.tlogs = t.tlogs[pickLen:]

	// update last flush\
	t.lastFlush = time.Now()

	return blocks, true
}

// Add adds tlog to tlog table
func (t *tlogTab) Add(tlb *schema.TlogBlock) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.tlogs = append(t.tlogs, tlb)
}

func (t *tlogTab) storeLastHash(rc redis.Conn, lastHash []byte) error {
	// store in memory
	t.lastHash = lastHash

	_, err := rc.Do("SET", t.lastHashKey(), t.lastHash)
	return err
}
func (t *tlogTab) getLastHash(rc redis.Conn) ([]byte, error) {
	// try to get from memory
	if len(t.lastHash) > 0 {
		return t.lastHash, nil
	}

	// get it from the store
	return redis.Bytes(rc.Do("GET", t.lastHashKey()))
}

func (t *tlogTab) lastHashKey() string {
	return lastHashPrefix + t.vdiskID
}
