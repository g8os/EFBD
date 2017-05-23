package tlogclient

import (
	"io"
)

type BufferedWriter interface {
	io.Writer
	Flush() error
}
