package config

import "errors"

// TODO deprecate or rethink these errors

var (
	// ErrNilSource is returned from any config API
	// function which received a nil source as its parameter.
	ErrNilSource = errors.New("no config source given")
	// ErrContextDone is returned from any config watch function
	// where the context is already finished,
	// while the watch function is still being created.
	ErrContextDone = errors.New("context is already done")
)

var (
	// ErrNilResource is returned when for a given config function
	// a nil resource was given by the user.
	ErrNilResource = errors.New("invalid nil resource given")
	// ErrNilID is returned when for a given config function
	// a nil string was given as the id.
	ErrNilID = errors.New("nil ID given")
)
