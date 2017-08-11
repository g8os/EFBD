/*Package zerolog is a package that prints messages in a format the 0-Core log monitor can read and use for logging, statistics, selfhealing and other features.

Usage:
	zerolog.Log(zerolog.LevelStdout, "Hello world")
Output:
	1::Hello world

Message

Accepted message types may very on provided log level:

String message (e.g.: LevelStdout, LevelStderr) takes strings, string aliases, types that implement fmt.Stringer, types that implement encoding.TextMarshaler.

JSON messages (e.g.: LevelJSON) takes any type that can be marshalled to JSON.

Additional info

package docs: https://github.com/zero-os/0-log/blob/master/README.md

Information about the 0-Core monitoring: https://github.com/zero-os/0-core/blob/master/docs/monitoring/logging.md
*/
package zerolog

import (
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
)

// Level represents the level that will be logged at
type Level uint8

const (
	// LevelStdout stdout
	LevelStdout Level = 1
	// LevelStderr stderr
	LevelStderr Level = 2
	// LevelJSON JSON result message
	LevelJSON Level = 20
)

var (
	// ErrLevelNotValid defines an error where the loggin level is not supported/valid
	ErrLevelNotValid = errors.New("logging level not valid")
	// ErrNilMessage represents an error where the supplied message was nil
	ErrNilMessage = errors.New("message was nil")
)

// Log prints a message in the 0-Core logging format
func Log(lvl Level, message interface{}) error {
	// check if message is nil/empty
	if message == nil {
		return ErrNilMessage
	}

	var msgStr string

	switch lvl {
	// string messages
	case LevelStdout, LevelStderr:
		var err error
		msgStr, err = msgString(message)
		if err != nil {
			return err
		}
	// json messages
	case LevelJSON:
		msgBs, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("could not marshal provided message into JSON: %s", err)
		}
		msgStr = string(msgBs)
	default:
		return ErrLevelNotValid
	}

	// print messages
	printLog(os.Stdout, lvl, msgStr)

	return nil
}

// checks if the interface can be turned into a string and returns it as such
func msgString(msg interface{}) (string, error) {
	// check if string
	if str, ok := msg.(string); ok {
		if str == "" {
			return "", ErrNilMessage
		}

		return str, nil
	}

	// check if implements fmt.Stringer
	if m, ok := msg.(fmt.Stringer); ok {
		return m.String(), nil
	}

	// check if implements encoding.TextMarshaler
	if m, ok := msg.(encoding.TextMarshaler); ok {
		str, err := m.MarshalText()
		if err != nil {
			return "", fmt.Errorf("could not MarshalText provided message: %s", err)
		}
		return string(str), nil
	}

	// check if msg reflects string
	if reflect.TypeOf(msg).Kind() == reflect.String {
		return reflect.ValueOf(msg).String(), nil
	}

	return "", fmt.Errorf("could not turn message into string")
}

// printLog formats the log and writes it to the io.Writer
func printLog(w io.Writer, lvl Level, msg string) {
	ml := isMultiline(msg)
	if ml {
		fmt.Fprintf(w, "%d:::\n%s\n:::\n", lvl, msg)
		return
	}

	fmt.Fprintf(w, "%d::%s\n", lvl, msg)
}

// isMultiline return true when a string constains \n
func isMultiline(str string) bool {
	return strings.Contains(str, "\n")
}
