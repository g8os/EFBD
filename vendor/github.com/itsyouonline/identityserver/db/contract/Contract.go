package contract

import (
	"github.com/itsyouonline/identityserver/db"
	"gopkg.in/mgo.v2/bson"
)

type Party struct {
	Type string
	Name string
}

type Contract struct {
	ID           bson.ObjectId `json:"-" bson:"_id,omitempty"`
	Content      string        `json:"content"`
	ContractType string        `json:"contractType"`
	Expires      db.DateTime   `json:"expires"`
	Extends      []string      `json:"extends"`
	Invalidates  []string      `json:"invalidates"`
	Parties      []Party       `json:"parties"`
	ContractId   string        `json:"contractId"`
	Signatures   []Signature   `json:"signatures"`
}
