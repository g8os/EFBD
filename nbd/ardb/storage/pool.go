package storage

import (
	"context"
	"fmt"
	"github.com/zero-os/0-Disk/log"
	"runtime"
	"sync"
)

type Work func(in interface{}) (out interface{})
type Callback func(out interface{})

type payload struct {
	in interface{}
	cb Callback
}

//Pool is a worker pool that grantees that if a job is submitted (Do call) *before* the pool is closed
//that it is granted to be executed, even if the pool has been closed immediately afterwards (from another go routine)
//
//Pool is defined by 2 arguments Workers which is number of workers, and Work function
type Pool struct {
	//Workers defines number of workers
	Workers int
	//Work function
	Work Work

	open   bool
	ch     chan payload
	m      sync.Mutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

//Do schedule a job for processing, will block if there are no free workers to immediately process your request.
//If Do is called while pool is open, and no free workers to process it, Do will block until a free worker is available,
//During that time, if the pool was closed, the Job will not get canceled, but no more jobs will be able to schedule.
//The call to close will block until all job on the queue is processed
//If cb is provided, it will get called with the output of the Work function.
func (p *Pool) Do(in interface{}, cb func(out interface{})) error {
	p.m.Lock()
	if !p.open {
		p.m.Unlock()
		return fmt.Errorf("pool is not open")
	}

	p.m.Unlock()
	p.ch <- payload{in, cb}
	return nil
}

func (p *Pool) apply(work payload) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("recovered from panic in work function: %v", err)
		}
	}()

	result := p.Work(work.in)
	if work.cb != nil {
		work.cb(result)
	}
}

func (p *Pool) workToEnd() {
	for {
		select {
		case w := <-p.ch:
			p.apply(w)
		default:
			return
		}
	}
}

func (p *Pool) work(ctx context.Context) {
	p.wg.Add(1)
	defer p.wg.Done()

	for {
		select {
		case w := <-p.ch:
			p.apply(w)
		case <-ctx.Done():
			//if context was canceled, select will choose the case to execute randomly if there
			//are waiting jobs on the queues, this is why we need to call workToEnd to make sure
			//we process all waiting jobs before returning.
			p.workToEnd()
			return
		}
	}
}

//Open prepares this pool, successive calls to Open will fail.
func (p *Pool) Open() error {
	p.m.Lock()
	defer p.m.Unlock()
	if p.open {
		return fmt.Errorf("pool is already open")
	}
	if p.Work == nil {
		return fmt.Errorf("undefined work function")
	}
	if p.Workers <= 0 {
		p.Workers = runtime.NumCPU()
	}

	p.open = true
	p.ch = make(chan payload)
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < p.Workers; i++ {
		go p.work(ctx)
	}

	p.cancel = cancel
	return nil
}

//Close closes the pool, return only when all workers process all waiting jobs. Close will prevent new jobs
//from being scheduled. Also successive calls to Close will fail
func (p *Pool) Close() error {
	//on close, we need to make sure that there are NO queued jobs
	//a close should return ONLY if all workers has processed all waiting jobs

	//prevent queuing of new jobs
	p.m.Lock()
	defer p.m.Unlock()
	if !p.open {
		return fmt.Errorf("pool is not open")
	}

	p.open = false
	p.cancel()
	p.wg.Wait()
	close(p.ch)

	return nil
}

//IsRunning returns true if pool is running.
func (p *Pool) IsRunning() bool {
	p.m.Lock()
	defer p.m.Unlock()
	return p.open
}
