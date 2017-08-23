package user

import (
	"github.com/itsyouonline/identityserver/db"
	"gopkg.in/validator.v2"
	"regexp"
)

type DigitalAssetAddress struct {
	CurrencySymbol string      `json:"currencysymbol"`
	Address        string      `json:"address"`
	Label          string      `json:"label" validate:"regexp=^[a-zA-Z\d\-_\s]{2,50}$"`
	Expire         db.DateTime `json:"expire"`
	Noexpiration   bool        `json:"noexpiration"`
}

func (d DigitalAssetAddress) Validate() bool {
	return validator.Validate(d) == nil && regexp.MustCompile(`^[a-zA-Z\d\-_\s]{2,50}$`).MatchString(d.Label)
}
