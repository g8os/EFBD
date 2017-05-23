package tlogclient

import (
	"io"
)

// writerFlusher is interface for io.Writer with Flush
type writerFlusher interface {
	io.Writer
	Flush() error
}
