package blockbuffer

import (
	"time"

	"github.com/g8os/blockstor/tlog/schema"
)

// entry defines a buffer entry
type entry struct {
	block    *schema.TlogBlock
	retryNum int
	timeout  time.Time
}

// newEntry creates a new buffer Entry
func newEntry(block *schema.TlogBlock, timeoutDur time.Duration) *entry {
	return &entry{
		block:    block,
		retryNum: 0,
		timeout:  time.Now().Add(timeoutDur),
	}
}

// update states of this entry
func (ent *entry) update(timeoutDur time.Duration) {
	ent.retryNum++
	ent.timeout = time.Now().Add(timeoutDur)
}

func (ent *entry) isTimeout(now time.Time) bool {
	return ent.timeout.Sub(now) <= 0
}

func (ent *entry) setTimeout() {
	ent.timeout = time.Now()
}
