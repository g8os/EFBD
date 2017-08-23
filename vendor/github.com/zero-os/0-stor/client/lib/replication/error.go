package replication

import (
	"strings"
)

// Error defines replication error
type Error struct {
	errs []error
}

// Error implements the error interface
func (e Error) Error() string {
	var errs []string
	for _, err := range e.errs {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ",")
}

// Errors returns it's underlying error
func (e Error) Errors() []error {
	return e.errs
}
