package distribution

import (
	"strings"
)

type Error struct {
	errs []error
}

func (e Error) Error() string {
	var errStrs []string
	for _, err := range e.errs {
		errStrs = append(errStrs, err.Error())
	}
	return strings.Join(errStrs, "--")
}
