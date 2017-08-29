package communication

import "github.com/itsyouonline/identityserver/db/user"

// MailService represent services that can send letters
type MailService interface {
	Send(address user.Address, message string) (err error)
}
