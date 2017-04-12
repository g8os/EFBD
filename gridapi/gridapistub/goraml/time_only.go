package goraml

import (
	"time"
)

var (
	timeOnlyFmt       = "15:04:05.99"
	timeOnlyFmtTicked = `"` + timeOnlyFmt + `"`
)

// TimeOnly represent RAML time-only type.
// The "partial-time" notation of RFC3339, namely hh:mm:ss[.ff...].
// Does not support date or time zone-offset notation.
type TimeOnly time.Time

// MarshalJSON override marshalJSON
func (to *TimeOnly) MarshalJSON() ([]byte, error) {
	return []byte(time.Time(*to).Format(timeOnlyFmtTicked)), nil
}

// UnmarshalJSON override unmarshalJSON
func (to *TimeOnly) UnmarshalJSON(b []byte) error {
	ts, err := time.Parse(timeOnlyFmtTicked, string(b))
	if err != nil {
		return err
	}

	*to = TimeOnly(ts)
	return nil
}

// String returns string representation
func (to *TimeOnly) String() string {
	return time.Time(*to).Format(timeOnlyFmt)
}
