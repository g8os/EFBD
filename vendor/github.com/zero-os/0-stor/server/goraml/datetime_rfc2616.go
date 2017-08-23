package goraml

import (
	"time"
)

var (
	dateTimeRFC2616Fmt       = "Mon, 02 Jan 2006 15:04:05 MST"
	dateTimeRFC2616FmtTicked = `"` + dateTimeRFC2616Fmt + `"`
)

// DateTimeRFC2616 is timestamp in RFC2616 format
type DateTimeRFC2616 time.Time

// MarshalJSON override marshalJSON
func (dt *DateTimeRFC2616) MarshalJSON() ([]byte, error) {
	return []byte(time.Time(*dt).Format(dateTimeRFC2616FmtTicked)), nil
}

// UnmarshalJSON override unmarshalJSON
func (dt *DateTimeRFC2616) UnmarshalJSON(b []byte) error {
	ts, err := time.Parse(dateTimeRFC2616FmtTicked, string(b))
	if err != nil {
		return err
	}

	*dt = DateTimeRFC2616(ts)
	return nil
}

// String returns it's string representation
func (dt *DateTimeRFC2616) String() string {
	return time.Time(*dt).Format(dateTimeRFC2616Fmt)
}
