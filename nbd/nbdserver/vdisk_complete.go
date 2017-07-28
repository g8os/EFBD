package main

import "sync"

// vdiskCompletion used to wait
// for vdisk completion
type vdiskCompletion struct {
	wg     sync.WaitGroup
	mux    sync.Mutex
	errors []error
}

func (vc *vdiskCompletion) Wait() []error {
	vc.wg.Wait()
	return vc.errors
}

func (vc *vdiskCompletion) Done() {
	vc.wg.Done()
}

func (vc *vdiskCompletion) AddError(err error) {
	vc.mux.Lock()
	defer vc.mux.Unlock()
	vc.errors = append(vc.errors, err)
}

func (vc *vdiskCompletion) Add() {
	vc.wg.Add(1)
}
