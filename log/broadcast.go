package log

import (
	"errors"

	"github.com/zero-os/0-log"
)

// Broadcast a message to the stderr in JSON format,
// such that managing services in an upper layer can react upon it.
func Broadcast(status MessageStatus, subject MessageSubject, data interface{}) {
	zerolog.Log(zerolog.LevelJSON, Message{
		Status:  status,
		Subject: subject,
		Data:    data,
	})
}

// Message defines the structure of all broadcast messages
// using the 0-log stderr logger.
type Message struct {
	Status  MessageStatus  `json:"status"`
	Subject MessageSubject `json:"subject"`
	Data    interface{}    `json:"data"`
}

// MessageSubject represents the subject of the message,
// which together with the status code should tell you
// how to interpret a message
type MessageSubject uint8

// String implements Stringer.String
func (s MessageSubject) String() string {
	switch s {
	case SubjectStorage:
		return subjectStorageStr
	case SubjectETCD:
		return subjectETCDStr
	case SubjectTlog:
		return subjectTlogStr
	case SubjectZeroStor:
		return subjectZeroStorStr
	default:
		return subjectNilStr
	}
}

// MarshalText implements encoding.TextMarshaler.MarshalText.
// Returns this message subject in string format.
func (s MessageSubject) MarshalText() ([]byte, error) {
	str := s.String()
	if str == subjectNilStr {
		return nil, errors.New("invalid message subject cannot be marshalled")
	}

	return []byte(str), nil
}

const (
	// SubjectStorage identifies the messages has to do with (ardb) storage
	SubjectStorage MessageSubject = iota
	// SubjectETCD identifies the messages has to do with etcd
	SubjectETCD
	// SubjectTlog identifies the messages has to do with tlog
	SubjectTlog
	// SubjectT0Stor identifies the messages has to do with 0-stor
	SubjectZeroStor
)

// subjects
const (
	subjectStorageStr  = "ardb"
	subjectETCDStr     = "etcd"
	subjectTlogStr     = "tlog"
	subjectZeroStorStr = "0-stor"
	subjectNilStr      = ""
)

// MessageStatus represents the status code
// that comes with a broadcast message
type MessageStatus uint

// status codes
const (
	StatusUnknownError   MessageStatus = 400
	StatusClusterTimeout MessageStatus = 401
	StatusServerTimeout  MessageStatus = 402
	StatusInvalidConfig  MessageStatus = 403
)

// InvalidConfigBody is the data given for a StatusInvalidConfig message.
type InvalidConfigBody struct {
	Endpoints []string `json:"endpoints"`
	Key       string   `json:"key"`
	// given if the config is only invalid
	// because it is used for a specific vdiskID
	// which has extra requirements the configs does not fullfill.
	VdiskID string `json:"vdiskID,omitempty"`
}
