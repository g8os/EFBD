// +build go1.9

package log

import (
	"fmt"
	"log/syslog"

	"github.com/go-stack/stack"
	log "github.com/inconshreveable/log15"
	"github.com/zero-os/0-Disk/errors"
)

// Handler interface defines where and how log records are written.
// Handlers are composable, providing you great flexibility in combining them
// to achieve the logging structure that suits your applications.
type Handler = log.Handler

// StderrHandler is the default handler for all logs,
// unless handlers are given
func StderrHandler() Handler {
	return log.StderrHandler
}

// FileHandler returns a handler which writes log records
// to the give file using the given format. If the path already exists,
// FileHandler will append to the given file.
// If it does not, FileHandler will create the file with mode 0644.
func FileHandler(path string) (Handler, error) {
	handler, err := log.FileHandler(path, log.LogfmtFormat())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create FileHandler")
	}

	return handler, nil
}

// SyslogHandler opens a connection to the system syslog daemon
// by calling syslog.New and writes all records to it.
func SyslogHandler(tag string) (Handler, error) {
	handler, err := log.SyslogHandler(syslog.LOG_KERN, tag, log.LogfmtFormat())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create SyslogHandler")
	}

	return handler, nil
}

// callerFileHandler returns a Handler that adds the line number and file of
// the calling function to the context with key "caller".
// It also updates the call stored in the record to the correct one.
func callerFileHandler(callDepth int, h Handler) Handler {
	return log.FuncHandler(func(r *Record) error {
		r.Call = stack.Caller(callDepth)
		r.Ctx = append(r.Ctx, "caller", fmt.Sprint(r.Call))
		return h.Log(r)
	})
}

// newLoggerHandler creates a new log handler.
// the specified level and above defines what is to be logged,
// the extraStackDepth ensures the correct file-source is attached to each log,
// and optionally handlers can defined.
// If no handlers are defined, the StderrHandler is used.
func newLoggerHandler(level Level, extraStackDepth int, handlers []Handler) log.Handler {
	var logHandler Handler

	if n := len(handlers); n == 0 {
		logHandler = log.StderrHandler
	} else if n == 1 {
		logHandler = handlers[0]
	} else {
		logHandler = log.MultiHandler(handlers...)
	}

	return log.LvlFilterHandler(log.Lvl(level),
		callerFileHandler(8+extraStackDepth, logHandler))
}
