package tlog

import (
	"time"
)

// TimeNow returns current UTC time
func TimeNow() time.Time {
	return time.Now().UTC()
}

// TimeNowTimestamp returns current timestamp
func TimeNowTimestamp() int64 {
	return TimeNow().UnixNano()
}
