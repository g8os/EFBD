package log

import (
	"errors"
	"fmt"
	"log/syslog"
	"net/smtp"
	"strings"

	valid "github.com/asaskevich/govalidator"
	log "github.com/inconshreveable/log15"
)

// Handler interface defines where and how log records are written.
// Handlers are composable, providing you great flexibility in combining them
// to achieve the logging structure that suits your applications.
type Handler interface {
	Log(r Record) error
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

// EmailHandler returns a handler which sends an email to the
// given email address in case a logged record is of the
// specified minimum level
func EmailHandler(minLevel Level, module string, to []string, from, smtp string, auth smtp.Auth) (Handler, error) {
	if auth == nil {
		return nil, errors.New("EmailHandler requires a valid and non-nil smtp Auth object")
	}
	if len(to) == 0 {
		return nil, errors.New("EmailHandler requires at least 1 recipient email address")
	}
	for _, t := range to {
		if !valid.IsEmail(t) {
			return nil, errors.New(t + " is not a valid recipient email address")
		}
	}
	if !valid.IsEmail(from) {
		return nil, errors.New(from + " is not a valid sender email address")
	}
	if !valid.IsDialString(smtp) {
		return nil, errors.New(smtp + " is not a valid dial smtp string")
	}

	return &emailHandler{
		minLevel: minLevel,
		module:   module,
		from:     from,
		to:       to,
		smtp:     smtp,
		auth:     auth,
	}, nil
}

type emailHandler struct {
	minLevel           Level
	module, from, smtp string
	to                 []string
	auth               smtp.Auth
}

func (handler *emailHandler) Log(r Record) error {
	if Level(r.Lvl) > handler.minLevel {
		return nil // no need to log
	}

	var context string
	if len(r.Ctx) > 0 {
		context = "Context:\r\n"
		ctxLen := len(r.Ctx)
		for i := 0; i < ctxLen; i += 2 {
			context += "\t\t- "
			if i+1 < ctxLen {
				context += fmt.Sprintf("%v: %v", r.Ctx[i], r.Ctx[i+1])
			} else {
				context += fmt.Sprint(r.Ctx[i])
			}
			context += "\r\n"
		}
	}

	var level string
	if r.Lvl == log.LvlError {
		level = "Fatal"
	} else {
		level = "Error"
	}

	messageParts := []string{
		"To: " + strings.Join(handler.to, ","),
		"Subject: Error in g8os/blockster mod " + handler.module + "!",
		"",
		"Be Aware!",
		"",
		"An error has occured in the g8os/blockstor mod " + handler.module + "!",
		"",
		"Time: " + r.Time.String(),
		"Level: " + level,
		context,
		"Message: " + r.Msg,
		"Callstack:",
		"",
		r.Call.String(),
		"",
		"- - -",
		"",
		"This is an automated Email send by the g8os/Blockstor email handler,",
		"please do no respond.",
		"",
	}
	message := strings.Join(messageParts, "\r\n")

	err := smtp.SendMail(
		handler.smtp, handler.auth,
		handler.from, handler.to,
		[]byte(message))
	if err != nil {
		return fmt.Errorf("couldn't send log over email: %s", err.Error())
	}

	return nil
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

func newLoggerHandler(level Level, handlers []Handler) log.Handler {
	var logHandler log.Handler
	if len(handlers) == 0 {
		logHandler = log.StderrHandler
	} else {
		handlerArr := []log.Handler{log.StderrHandler}
		for _, handler := range handlers {
			var lh log.Handler
			if l, ok := handler.(*fromLog15Handler); ok {
				lh = l.internal
			} else {
				lh = &toLog15Handler{handler}
			}
			handlerArr = append(handlerArr, lh)
		}
		logHandler = log.MultiHandler(handlerArr...)
	}

	return log.LvlFilterHandler(log.Lvl(level),
		log.CallerFileHandler(logHandler))
}
