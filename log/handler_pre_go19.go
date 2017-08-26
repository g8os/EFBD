// +build !go1.9

package log

import (
	"fmt"
	"log/syslog"

	"github.com/go-stack/stack"
	log "github.com/inconshreveable/log15"
)

// Handler interface defines where and how log records are written.
// Handlers are composable, providing you great flexibility in combining them
// to achieve the logging structure that suits your applications.
type Handler interface {
	Log(r Record) error
}

// StderrHandler is the default handler for all logs,
// unless handlers are given
func StderrHandler() Handler {
	return &fromLog15Handler{log.StderrHandler}
}

// FileHandler returns a handler which writes log records
// to the give file using the given format. If the path already exists,
// FileHandler will append to the given file.
// If it does not, FileHandler will create the file with mode 0644.
func FileHandler(path string) (Handler, error) {
	handler, err := log.FileHandler(path, log.LogfmtFormat())
	if err != nil {
		return nil, fmt.Errorf("couldn't create FileHandler: %s", err.Error())
	}

	return &fromLog15Handler{handler}, nil
}

// SyslogHandler opens a connection to the system syslog daemon
// by calling syslog.New and writes all records to it.
func SyslogHandler(tag string) (Handler, error) {
	handler, err := log.SyslogHandler(syslog.LOG_KERN, tag, log.LogfmtFormat())
	if err != nil {
		return nil, fmt.Errorf("couldn't create SyslogHandler: %s", err.Error())
	}

	return &fromLog15Handler{handler}, nil
}

// toLog15Handler is used to map our Handler type
// to the log15.Handler type
type toLog15Handler struct {
	internal Handler
}

// Log implements log15.Handler.Log
func (handler *toLog15Handler) Log(r *log.Record) error {
	return handler.internal.Log(Record(r))
}

// fromLog15Handler is used to map the log15.Handler type
// to our Handler type
type fromLog15Handler struct {
	internal log.Handler
}

// Log implements Handler.Log
func (handler *fromLog15Handler) Log(r Record) error {
	return handler.internal.Log((*log.Record)(r))
}

// callerFileLog15Handler returns a log15Handler that adds the line number and file of
// the calling function to the context with key "caller".
// It also updates the call stored in the record to the correct one.
func callerFileLog15Handler(callDepth int, h log.Handler) log.Handler {
	return log.FuncHandler(func(r *log.Record) error {
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
	var logHandler log.Handler

	if len(handlers) == 0 {
		logHandler = log.StderrHandler
	} else {
		handlerArr := []log.Handler{}
		for _, handler := range handlers {
			var lh log.Handler
			if l, ok := handler.(*fromLog15Handler); ok {
				lh = l.internal
			} else {
				lh = &toLog15Handler{handler}
			}
			handlerArr = append(handlerArr, lh)
		}

		if len(handlerArr) == 1 {
			logHandler = handlerArr[0]
		} else {
			logHandler = log.MultiHandler(handlerArr...)
		}
	}

	return log.LvlFilterHandler(log.Lvl(level),
		callerFileLog15Handler(8+extraStackDepth, logHandler))
}
