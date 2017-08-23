package user

import (
	log "github.com/Sirupsen/logrus"
	"regexp"
)

type Label string

func IsValidLabel(label string) (valid bool) {
	labelRegex := regexp.MustCompile(`^[a-zA-Z\d\-_\s]{2,50}$`)
	valid = labelRegex.MatchString(label)

	if !valid {
		log.Debug("Invalid label: ", label)
	}
	return valid
}
