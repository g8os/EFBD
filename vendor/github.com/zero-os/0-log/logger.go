/*Package zerolog is a package that prints messages in a format the 0-Core log monitor can read and use for logging (errors), statistics, selfhealing and other features.

Usage:
	zerolog.Log(zerolog.LevelStdout, "Hello world")
Output:
	1::Hello world

Message

Accepted message types may very on provided log level:

String message (e.g.: LevelStdout, LevelStderr) takes strings, string aliases, types that implement fmt.Stringer, types that implement encoding.TextMarshaler.

Statistics message (e.g: LevelStatistics) takes a MsgStatistics to have fields and validation for data required by the 0-Core statistics monitor
https://github.com/zero-os/0-core/blob/master/docs/monitoring/stats.md

The MsgStatistics Operation field takes an AggregationType which defines the data aggregation strategy for the 0-core

The MsgStatistics Tags field takes a MetricTags type which is a map with a string as key and an interface as value. When logging this map is formatted to a flat string, if the value is a string it will simply be added to the formatted string, if not it will check the value implements the fmt.Stringer or encoding.TextMarshaler interfaces, as a last resort the value will be turned into a string using fmt.Sprint.

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
	// LevelStatistics statistics/monitoring message
	LevelStatistics Level = 10
	// LevelJSON JSON result message
	LevelJSON Level = 20
)

var (
	// ErrLevelNotValid defines an error where the loggin level is not supported/valid
	ErrLevelNotValid = errors.New("logging level not valid")
	// ErrNilMessage represents an error where the supplied message was nil
	ErrNilMessage = errors.New("message was nil")
	// ErrInvalidMessage represents an error where the supplied message was invalid
	ErrInvalidMessage = errors.New("message was invalid")
	// ErrNilStatisticsKey represents an error where the supplied
	// statistics message has no key specified
	ErrNilStatisticsKey = errors.New("statistics key was missing")
	// ErrInvalidAggregationType represents an error where the supplied
	// statistics message has an invalid aggregation type specified
	ErrInvalidAggregationType = errors.New("invalid aggregation type")
)

// Log prints a message in the 0-Core logging format
func Log(lvl Level, message interface{}) error {
	var msgStr string
	var err error

	switch lvl {
	// string messages
	case LevelStdout, LevelStderr:
		msgStr, err = msgString(message)
		if err != nil {
			return err
		}
	// stats messages
	case LevelStatistics:
		msgStr, err = msgStatistics(message)
		if err != nil {
			return err
		}
	// json messages
	case LevelJSON:
		msgStr, err = msgJSON(message)
		if err != nil {
			return err
		}
	default:
		return ErrLevelNotValid
	}

	// print messages
	printLog(os.Stdout, lvl, msgStr)

	return nil
}

// msgString checks if the interface can be turned into a string and returns it as such
func msgString(msg interface{}) (string, error) {
	if msg == nil {
		return "", ErrNilMessage
	}
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
			return "", ErrInvalidMessage
		}
		return string(str), nil
	}

	// check if msg reflects string
	if reflect.TypeOf(msg).Kind() == reflect.String {
		return reflect.ValueOf(msg).String(), nil
	}

	return "", ErrInvalidMessage
}

// msgStatistics validates and formats a statistics log message
// Validates if message conforms to 0-core statistics spec:
// https://github.com/zero-os/0-core/blob/master/docs/monitoring/stats.md
func msgStatistics(msg interface{}) (string, error) {
	// check if msg is type MsgStatistics
	statMsg, ok := msg.(MsgStatistics)
	if !ok {
		return "", ErrInvalidMessage
	}

	err := statMsg.Validate()
	if err != nil {
		return "", err
	}

	str := fmt.Sprintf("%s:%f|%s",
		statMsg.Key, statMsg.Value, statMsg.Operation)

	if len(statMsg.Tags) != 0 {
		str = str + "|" + statMsg.Tags.String()
	}

	return str, nil
}

// msgJSON validates and formats a JSON result message
func msgJSON(msg interface{}) (string, error) {
	if msg == nil {
		return "", ErrNilMessage
	}
	msgBs, err := json.Marshal(msg)
	if err != nil {
		return "", ErrInvalidMessage
	}

	return string(msgBs), nil
}

// MsgStatistics represents the data needed for a statistics message
type MsgStatistics struct {
	Key       string
	Value     float64
	Operation AggregationType
	Tags      MetricTags
}

// Validate validates the MsgStatistics according to spec:
// https://github.com/zero-os/0-core/blob/master/docs/monitoring/stats.md
func (msg *MsgStatistics) Validate() error {
	if msg.Key == "" {
		return ErrNilStatisticsKey
	}
	err := msg.Operation.Validate()
	return err
}

// AggregationType represents an statistics aggregation type
type AggregationType string

const (
	// AggregationAverages represents an averaging aggregation type
	AggregationAverages = AggregationType("A")
	// AggregationDifferentiates represents a differentiating aggregation type
	AggregationDifferentiates = AggregationType("D")
)

// Validate validates the AggregationType
func (at AggregationType) Validate() error {
	switch at {
	case AggregationAverages, AggregationDifferentiates:
		return nil
	default:
		return ErrInvalidAggregationType
	}

}

// MetricTags represents statistics metric tags
type MetricTags map[string]interface{}

// String converts the MetricTags into a flat string for logging
func (mt MetricTags) String() string {
	if len(mt) < 1 {
		return ""
	}

	var str string
	for k, v := range mt {
		str = str + k + "=" + tagValString(v) + ","
	}

	// return without last tag seperator
	return str[:len(str)-1]
}

// TagValString converts a MetricTags value into a string
func tagValString(v interface{}) string {
	// check string
	if str, ok := v.(string); ok {
		return str
	}

	// check if implements fmt.Stringer interface
	if s, ok := v.(fmt.Stringer); ok {
		return s.String()
	}

	// check if implements encoding.TextMarshaller interface
	if s, ok := v.(encoding.TextMarshaler); ok {
		str, err := s.MarshalText()
		if err == nil {
			return string(str)
		}
		// if err use fmt.Sprint
	}

	// check if byte slice
	if bs, ok := v.([]byte); ok {
		return string(bs)
	}

	// last resort
	return fmt.Sprint(v)
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
