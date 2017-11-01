package decoder

import (
	"github.com/zero-os/0-Disk/errors"
)

var (
	// ErrNilLastHash indicates that there is no last hash entry
	ErrNilLastHash = errors.New("nil last hash")
)
