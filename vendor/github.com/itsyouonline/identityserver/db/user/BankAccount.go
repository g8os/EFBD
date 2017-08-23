package user

import (
	"regexp"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/validator.v2"
)

type BankAccount struct {
	Bic     string `json:"bic"`
	Country string `json:"country"`
	Iban    string `json:"iban"`
	Label   string `json:"label" validate:"regexp=^[a-zA-Z\d\-_\s]{2,50}$"`
}

func (bank BankAccount) Validate() bool {
	valid := validator.Validate(bank) == nil && regexp.MustCompile(`^[a-zA-Z\d\-_\s]{2,50}$`).MatchString(bank.Label)
	if len(bank.Bic) != 8 && len(bank.Bic) != 11 {
		log.Debug("Invalid bic: ", bank.Bic)
		return false
	}
	if len(bank.Iban) > 30 || len(bank.Iban) < 1 {
		log.Debug("Invalid iban: ", bank.Iban)
		return false
	}
	if len(bank.Country) > 40 || len(bank.Country) < 0 {
		log.Debug("Invalid country: ", bank.Country)
		return false
	}
	return valid
}
