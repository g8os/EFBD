package tlogclient

import (
	"io"
)

type bufferedWriter interface {
	io.Writer
	Flush() error
}
