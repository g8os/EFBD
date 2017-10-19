package config

import "errors"

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

var (
	// ErrSourceUnavailable is returned when the given source
	// is unavailable due to any kind of critical failure.
	ErrSourceUnavailable = errors.New("config: source is unavailable")
	// ErrConfigUnavailable is returned when a requested config
	// could not be found in the given source.
	ErrConfigUnavailable = errors.New("config is not available in source")
	// ErrInvalidKey is returned when a config is requested using
	// a config key which is either invalid or not supported by
	// the given source.
	ErrInvalidKey = errors.New("config key is invalid or not supported")
)

var (
	// ErrInvalidStorageServerConfig is returned when a storage server config in invalid
	ErrInvalidStorageServerConfig = errors.New("invalid storage server config")
)

// NewInvalidConfigError creates a new InvalidConfigError from a given error
func NewInvalidConfigError(err error) *InvalidConfigError {
	return &InvalidConfigError{err}
}

// InvalidConfigError is returned when a config read from a source is invalid,
// because its an illegal config or because it is not acceptable for the current
// vdisk usage.
type InvalidConfigError struct {
	internalErr error
}

// Error implements Error.Error
func (err *InvalidConfigError) Error() string {
	if err == nil {
		return ""
	}

	if err.internalErr == nil {
		return "invalid config"
	}

	return "invalid config: " + err.internalErr.Error()
}
