package main

import (
	"context"
	"sync"
)

// vdiskCompletion used to wait
// for vdisk completion
type vdiskCompletion struct {
	wg         sync.WaitGroup
	mux        sync.Mutex
	addMux     sync.Mutex
	errors     []error
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func newVdiskCompletion() *vdiskCompletion {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &vdiskCompletion{
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

// Wait for all vdisks to be finished.
func (vc *vdiskCompletion) Wait() []error {
	vc.addMux.Lock()
	defer vc.addMux.Unlock()
	vc.wg.Wait()

	return vc.evictAllErrors()
}

// Stop all vdisks
func (vc *vdiskCompletion) StopAll() {
	vc.cancelFunc()
}

// instructs vdisk to stop
// work like context.Done
func (vc *vdiskCompletion) Stopped() <-chan struct{} {
	return vc.ctx.Done()
}

// mark this vdisk as finished
func (vc *vdiskCompletion) Done() {
	vc.wg.Done()
}

func (vc *vdiskCompletion) AddError(err error) {
	vc.mux.Lock()
	defer vc.mux.Unlock()
	vc.errors = append(vc.errors, err)
}

// register a vdisk
func (vc *vdiskCompletion) Add() {
	vc.addMux.Lock()
	defer vc.addMux.Unlock()
	vc.wg.Add(1)
}

func (vc *vdiskCompletion) evictAllErrors() []error {
	vc.mux.Lock()
	defer vc.mux.Unlock()

	errors := vc.errors
	vc.errors = nil
	return errors
}
