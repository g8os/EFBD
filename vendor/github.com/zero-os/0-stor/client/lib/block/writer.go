package block

import (
	"github.com/zero-os/0-stor/client/meta"
)

// Writer defines Writer that work on block level
type Writer interface {
	// WriteBlock write the value to the underlying writer
	WriteBlock(key, value []byte, md *meta.Meta) (*meta.Meta, error)
}
