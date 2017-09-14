package tlog

import (
	"time"
)

const (
	// FlushWaitRetry defines the timeout for retry flush
	FlushWaitRetry = time.Minute
	// FlushWaitRetryNum defines the number of flush retries
	FlushWaitRetryNum = 4
)
