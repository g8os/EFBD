package communication

import (
	log "github.com/Sirupsen/logrus"
	"github.com/itsyouonline/identityserver/db/user"
)

type DevMailService struct{}

// Send sends a mail
func (s *DevMailService) Send(address user.Address, message string) (err error) {
	log.Infof("In production a letter would be sent to %s with the following content:\n%s", address, message)
	return
}
