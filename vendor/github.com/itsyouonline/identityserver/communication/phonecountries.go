package communication

import "strings"

// IsRussianMobileNumber checks if a phone number is a russian mobile. phone numbers need
// to be passed in E.164 format (leading '+'').
func IsRussianMobileNumber(phonenumber string) bool {
	// Russian phone number has a 1 digit country code, 10 digit national significant
	// number, and a '+' sign as we accept only international numbers in E.164 format
	// TODO: possibly more fine grained verification
	if len(phonenumber) == 12 && strings.HasPrefix(phonenumber, "+7") {
		return true
	}
	return false
}
