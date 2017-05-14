package log

import (
	"fmt"
	"os"
	"sync"

	log "github.com/inconshreveable/log15"
)

// Record is what a Logger asks its handler to write
type Record *log.Record

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

var (
	stdMux      sync.Mutex
	stdLevel    = InfoLevel
	stdHandlers []Handler
	std         = New("global", InfoLevel).(*glueLogger)
)

// SetLevel defines at which level the std logger should log
func SetLevel(level Level) {
	stdMux.Lock()
	stdLevel = level
	stdMux.Unlock()
	std.internal.SetHandler(newLoggerHandler(stdLevel, stdHandlers))
}

// GetLevel returns the level used by the std logger
func GetLevel() Level {
	stdMux.Lock()
	defer stdMux.Unlock()
	return stdLevel
}

// SetHandlers allows you to set extra handlers on the std logger,
// besides the default StdErr Logger
func SetHandlers(handlers ...Handler) {
	stdMux.Lock()
	stdHandlers = handlers
	stdMux.Unlock()

	std.internal.SetHandler(newLoggerHandler(stdLevel, stdHandlers))
}

// Debug logs a message at level Debug on the standard logger.
func Debug(args ...interface{}) {
	std.Debug(args...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, args ...interface{}) {
	std.Debugf(format, args...)
}

// Info logs a message at level Info on the standard logger.
func Info(args ...interface{}) {
	std.Info(args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, args ...interface{}) {
	std.Infof(format, args...)
}

// Error logs a message at level Error on the standard logger.
func Error(args ...interface{}) {
	std.Error(args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, args ...interface{}) {
	std.Errorf(format, args...)
}

// Fatal logs a message at level Fatal on the standard logger.
func Fatal(args ...interface{}) {
	std.Fatal(args...)
}

// Fatalf logs a message at level Fatal on the standard logger.
func Fatalf(format string, args ...interface{}) {
	std.Fatalf(format, args...)
}

// New logger, creates a new logger
func New(module string, level Level, handlers ...Handler) Logger {
	logger := log.New("module", module)
	logger.SetHandler(newLoggerHandler(level, handlers))

	return &glueLogger{logger}
}

// NopLogger creates a Logger which discards all logs,
// and doesn't ever exit the process when a fatal error occurs.
func NopLogger() Logger {
	return new(nopLogger)
}

// Logger defines a pragmatic Logger interface.
type Logger interface {
	// verbose messages targeted at the developer
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	// info messages targeted at the user and developer
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	// error messages targeted at the user, sysadmin and developer,
	// but mostly at the sysadmin
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	// a fatal message targeted at the user and developer
	// the program will exit as this message
	// this level shouldn't be used by libraries
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

type glueLogger struct {
	internal log.Logger
}

// Debug implements Logger.Debug
func (logger *glueLogger) Debug(args ...interface{}) {
	logger.internal.Debug(fmt.Sprint(args...))
}

// Debugf implements Logger.Debugf
func (logger *glueLogger) Debugf(format string, args ...interface{}) {
	logger.internal.Debug(fmt.Sprintf(format, args...))
}

// Info implements Logger.Info
func (logger *glueLogger) Info(args ...interface{}) {
	logger.internal.Info(fmt.Sprint(args...))
}

// Infof implements Logger.Infof
func (logger *glueLogger) Infof(format string, args ...interface{}) {
	logger.internal.Info(fmt.Sprintf(format, args...))
}

// Error implements Logger.Error
func (logger *glueLogger) Error(args ...interface{}) {
	logger.internal.Error(fmt.Sprint(args...))
}

// Errorf implements Logger.Errorf
func (logger *glueLogger) Errorf(format string, args ...interface{}) {
	logger.internal.Error(fmt.Sprintf(format, args...))
}

// Fatal implements Logger.Fatal
func (logger *glueLogger) Fatal(args ...interface{}) {
	logger.internal.Crit(fmt.Sprint(args...))
	os.Exit(1)
}

// Fatalf implements Logger.Fatalf
func (logger *glueLogger) Fatalf(format string, args ...interface{}) {
	logger.internal.Crit(fmt.Sprintf(format, args...))
	os.Exit(1)
}

type nopLogger struct{}

func (logger *nopLogger) Debug(args ...interface{})                 {}
func (logger *nopLogger) Debugf(format string, args ...interface{}) {}
func (logger *nopLogger) Info(args ...interface{})                  {}
func (logger *nopLogger) Infof(format string, args ...interface{})  {}
func (logger *nopLogger) Error(args ...interface{})                 {}
func (logger *nopLogger) Errorf(format string, args ...interface{}) {}
func (logger *nopLogger) Fatal(args ...interface{})                 {}
func (logger *nopLogger) Fatalf(format string, args ...interface{}) {}
