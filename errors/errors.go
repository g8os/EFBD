/*Package errors defines a package used for error handling in 0-Disk.
It provides an error type that has the original error as the cause,
more context to the error can be provided by wrapping the error with an additional message.
The underlying error (cause) will then be preserved and can be fetched (and then checked)
with the Cause function.
*/
package errors

import (
	"fmt"

	"github.com/pkg/errors"
)

// New returns an error with provided message
// and records the stack trace at the point where it was called.
func New(msg string) error {
	return errors.New(msg)
}

// Newf formats according to a format specifier
// and returns the string as a value that satisfies error.
// It also records the stack trace at the point where it was called.
func Newf(format string, args ...interface{}) error {
	return errors.Errorf(format, args...)
}

// Wrap returns an error that is annotated with provided message
// If error is nil, Wrap returns nil
func Wrap(err error, msg string) error {
	return errors.WithMessage(err, msg)
}

// Wrapf returns an error that is annotated with provided message
// If err is nil, Wrapf returns nil.
func Wrapf(err error, format string, args ...interface{}) error {
	message := fmt.Sprintf(format, args...)
	return errors.WithMessage(err, message)
}

// Cause returns the underlying cause of the error if possible.
// If the error does not implement `Cause() error` it returns the full error
func Cause(err error) error {
	return errors.Cause(err)
}
