package main

import (
	"time"
)

const (
	flushWaitRetry    = time.Minute // retry flush after this timeout
	flushWaitRetryNum = 4           // retry flush for this times
)
