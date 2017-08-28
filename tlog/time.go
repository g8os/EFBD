package tlog

import (
	"time"
)

// TimeNow returns current UTC time
func TimeNow() time.Time {
	return time.Now().UTC()
}

// TimeNowTimestamp returns current timestamp
func TimeNowTimestamp() uint64 {
	return uint64(TimeNow().UnixNano())
}
