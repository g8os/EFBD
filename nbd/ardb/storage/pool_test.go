package storage

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var done int32
	work := func(in interface{}) interface{} {
		select {
		case <-ctx.Done():
		}
		atomic.AddInt32(&done, 1)

		return nil
	}

	p := &Pool{
		Workers: 5,
		Work:    work,
	}

	if err := p.Open(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			wg.Done()
			if err := p.Do(i, nil); err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	wg.Wait() //wait until we sure all go routines started
	//extra wait just to be sure
	<-time.After(100 * time.Millisecond)

	//release all workers make them do their work
	cancel()
	p.Close()

	if ok := assert.Equal(t, int32(100), done); !ok {
		t.Fatal()
	}

	if err := p.Do(200, nil); err == nil {
		t.Fatal("expected err")
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}
