package config

import "github.com/zero-os/0-Disk/errors"

// Error constants
var (
	// ErrNilSource is returned from any config API
	// function which received a nil source as its parameter.
	ErrNilSource = errors.New("no config source given")

	// ErrContextDone is returned from any config watch function
	// where the context is already finished,
	// while the watch function is still being created.
	ErrContextDone = errors.New("context is already done")

	// ErrNilResource is returned when for a given config function
	// a nil resource was given by the user.
	ErrNilResource = errors.New("invalid nil resource given")

	// ErrNilID is returned when for a given config function
	// a nil string was given as the id.
	ErrNilID = errors.New("nil ID given")

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

	// ErrNilConfig is returned when the given config was nil
	ErrNilConfig = errors.New("config is nil")

	// ErrInvalidConfig is returned when the given config was invalid
	ErrInvalidConfig = errors.New("config is invalid")

	// ErrInvalidStorageCluster is returned when the storage cluster is invalid
	ErrInvalidStorageCluster = errors.New("storage cluster is invalid")

	// ErrNilStorage is returned when a storage was nil while being required
	ErrNilStorage = errors.New("storage is nil while it is required")

	// ErrInvalidDatabase is returned when the StorageServerConfig's database is invalid
	ErrInvalidDatabase = errors.New("invalid database")
)

// NewInvalidConfigError creates a new error with ErrInvalidConfig as Cause
// with the provided error as wrapped context
func NewInvalidConfigError(err error) error {
	return errors.Wrap(ErrInvalidConfig, err.Error())
}
