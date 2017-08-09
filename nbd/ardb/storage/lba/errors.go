package lba

import "errors"

var (
	// errBucketIsEmpty is an error returned when
	// the code tries to auto-evict, while the bucket is empty.
	errBucketIsEmpty = errors.New("bucket is empty")

	// internal error which is returned
	// in case a pure nil sector is being written
	errNilSectorWrite = errors.New("sector is nil, and cannot be written")
)

// flushError is a collection of errors received,
// returned by the flush command if one or more sectors couldn't be flushed
type flushError []error

// add an error to this list of flush errors,
func (e *flushError) AddError(err error) {
	if err == nil {
		return
	}

	*e = append(*e, err)
}

// return this list as an error,
// only if it actually contains any errors.
func (e flushError) AsError() error {
	if len(e) == 0 {
		return nil
	}

	return e
}

// Error implements Error.Error
func (e flushError) Error() (s string) {
	s = "flush failed to flush one or multiple sectors:"
	for _, err := range e {
		s += `"` + err.Error() + `";`
	}
	return
}
