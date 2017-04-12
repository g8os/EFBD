package goraml

import (
	"time"
)

var (
	datetimeOnlyFmt       = "2006-01-02T15:04:05.99"
	datetimeOnlyFmtTicked = `"` + datetimeOnlyFmt + `"`
)

// DatetimeOnly represent RAML datetime-only type
// Combined date-only and time-only with a separator of "T",
// namely yyyy-mm-ddThh:mm:ss[.ff...]. Does not support a time zone offset.
type DatetimeOnly time.Time

// MarshalJSON override marshalJSON
func (dto *DatetimeOnly) MarshalJSON() ([]byte, error) {
	return []byte(time.Time(*dto).Format(datetimeOnlyFmtTicked)), nil
}

// UnmarshalJSON override unmarshalJSON
func (dto *DatetimeOnly) UnmarshalJSON(b []byte) error {
	ts, err := time.Parse(datetimeOnlyFmtTicked, string(b))
	if err != nil {
		return err
	}

	*dto = DatetimeOnly(ts)
	return nil
}

// String returns string representation
func (dto *DatetimeOnly) String() string {
	return time.Time(*dto).Format(datetimeOnlyFmt)
}
