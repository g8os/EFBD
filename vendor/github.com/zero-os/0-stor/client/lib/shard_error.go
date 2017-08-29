package lib

import (
	"encoding/json"
	"sync"
)

const (
	ShardType0Stor = "0-stor"
	ShardTypeEtcd  = "etcd"
)

const (
	// StatusUnknownError means that we don't know exactly what the error is
	StatusUnknownError = 400

	// StatusTimeoutError means that the operation to that shard got timed out
	StatusTimeoutError = 401

	// StatusInvalidShardAddress means that shard address is invalid
	// which make us unable to creates the client for it.
	// It could only happen if the metadata is invalid
	StatusInvalidShardAddress = 402
)

type serverError struct {
	Addrs []string `json:"addrs"`
	Kind  string   `json:"kind"`
	Err   error    `json:"error"`
	Code  int      `json:"code"`
}

// ShardError represents errors in a shard
// It implements error interface on which the `Error` func
// returns JSON string which make it easy to be parsed by other module
// or language
type ShardError struct {
	errors []serverError
	mux    sync.Mutex
}

// Add adds an error, it is goroutine safe
func (se *ShardError) Add(addrs []string, kind string, err error, code int) {
	se.mux.Lock()
	defer se.mux.Unlock()

	if code == 0 {
		code = StatusUnknownError
	}
	se.errors = append(se.errors, serverError{
		Addrs: addrs,
		Kind:  kind,
		Err:   err,
		Code:  code,
	})
}

// Nil returns true if it is a nil error
func (se ShardError) Nil() bool {
	se.mux.Lock()
	defer se.mux.Unlock()

	return len(se.errors) == 0
}

func (se ShardError) Num() int {
	se.mux.Lock()
	defer se.mux.Unlock()

	return len(se.errors)
}

// Error implements error interface
func (se ShardError) Error() string {
	se.mux.Lock()
	defer se.mux.Unlock()

	b, err := json.Marshal(se.errors)
	if err != nil {
		// This error won't happen in case we strict to use non interface{} fields
		// TODO : returns something better instead of error returned from JSON
		return err.Error()
	}
	return string(b)
}
