package decoder

import (
	"errors"
)

var (
	// ErrNilLastHash indicates that there is no last hash entry
	ErrNilLastHash = errors.New("nil last hash")
)
