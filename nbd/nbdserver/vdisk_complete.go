package main

import "sync"

// vdiskCompletion used to wait
// for vdisk completion
type vdiskCompletion struct {
	wg     sync.WaitGroup
	mux    sync.Mutex
	addMux sync.RWMutex
	errors []error
}

func (vc *vdiskCompletion) Wait() []error {
	vc.addMux.Lock()
	defer vc.addMux.Unlock()
	vc.wg.Wait()

	return vc.evictAllErrors()
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
	vc.addMux.RLock()
	defer vc.addMux.RUnlock()
	vc.wg.Add(1)
}

func (vc *vdiskCompletion) evictAllErrors() []error {
	vc.mux.Lock()
	defer vc.mux.Unlock()

	errors := vc.errors
	vc.errors = nil
	return errors
}
