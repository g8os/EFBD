package main

import (
	"sync"
	"time"
)

// tlog table
type tlogTab struct {
	tlogs     []*TlogBlock
	lastFlush time.Time
	lock      sync.RWMutex
}

func newTlogTab(volID uint32) *tlogTab {
	return &tlogTab{
		tlogs:     []*TlogBlock{},
		lastFlush: time.Now(),
	}
}

// check if this volume ID need to be flushed
func (t *tlogTab) needFlush(flushSize, flushTime int, periodic bool) bool {
	if len(t.tlogs) < flushSize {
		return false
	}
	if periodic && int(time.Since(t.lastFlush).Seconds()) < flushTime {
		return false
	}
	return true
}

// pick tlogs to be flushed, if possible
func (t *tlogTab) Pick(flushSize, flushTime int, periodic bool) ([]*TlogBlock, bool) {
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

	return blocks, true
}

// Add adds tlog to tlog table
func (t *tlogTab) Add(tlb *TlogBlock) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.tlogs = append(t.tlogs, tlb)
}