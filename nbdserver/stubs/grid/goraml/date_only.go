package goraml

import (
	"time"
)

var (
	dateOnlyFmt       = "2006-01-02"
	dateOnlyFmtTicked = `"` + dateOnlyFmt + `"`
)

// DateOnly represent RAML date-only type
// The "full-date" notation of RFC3339, namely yyyy-mm-dd.
// Does not support time or time zone-offset notation.
type DateOnly time.Time

// MarshalJSON override marshalJSON
func (do *DateOnly) MarshalJSON() ([]byte, error) {
	return []byte(time.Time(*do).Format(dateOnlyFmtTicked)), nil
}

// UnmarshalJSON override unmarshalJSON
func (do *DateOnly) UnmarshalJSON(b []byte) error {
	ts, err := time.Parse(dateOnlyFmtTicked, string(b))
	if err != nil {
		return err
	}

	*do = DateOnly(ts)
	return nil
}

// String returns string representation
func (do *DateOnly) String() string {
	return time.Time(*do).Format(dateOnlyFmt)
}
