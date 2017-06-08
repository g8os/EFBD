package blockbuffer

import (
	"time"

	"github.com/zero-os/0-Disk/tlog/schema"
)

// entry defines a buffer entry
type entry struct {
	block    *schema.TlogBlock
	retryNum int
	timeout  int64
}

// newEntry creates a new buffer Entry
func newEntry(block *schema.TlogBlock, timeoutDur time.Duration) *entry {
	return &entry{
		block:    block,
		retryNum: 0,
		timeout:  time.Now().Add(timeoutDur).UnixNano(),
	}
}

// update states of this entry
func (ent *entry) update(timeoutDur time.Duration, retryInc int) {
	ent.retryNum += retryInc
	ent.timeout = time.Now().Add(timeoutDur).UnixNano()
}

func (ent *entry) isTimeout(now time.Time) bool {
	return ent.timeout-now.UnixNano() <= 0
}

func (ent *entry) setTimeout() {
	ent.timeout = time.Now().UnixNano()
}
