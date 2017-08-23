package contract

import "github.com/itsyouonline/identityserver/db"

type Signature struct {
	Date      db.DateTime `json:"date"`
	PublicKey string      `json:"publicKey"`
	Signature string      `json:"signature"`
	SignedBy  string      `json:"signedBy"`
}
