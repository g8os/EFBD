package common

import (
	"time"
)

type Reservation struct {
	ID           string
	AdminID      string
	Created      time.Time
	ExpireAt     time.Time
	SizeReserved float64
	SizeUsed     float64
	Updated      time.Time
}
