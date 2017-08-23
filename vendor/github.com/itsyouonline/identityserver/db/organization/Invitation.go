package organization

import "github.com/itsyouonline/identityserver/db"

type Invitation struct {
	Created db.DateTime `json:"created"`
	Role    string      `json:"role"`
	User    string      `json:"user"`
}
