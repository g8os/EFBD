// +build !go1.9

package log

import (
	log "github.com/inconshreveable/log15"
)

// Level type
type Level log.Lvl

const (
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel = Level(log.LvlDebug)
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel = Level(log.LvlInfo)
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel = Level(log.LvlError)
	// FatalLevel level. Logs and then calls `os.Exit(1)`.
	FatalLevel = Level(log.LvlCrit)
)

// Record is what a Logger asks its handler to write
type Record *log.Record
