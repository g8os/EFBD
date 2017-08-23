package contract

import "gopkg.in/mgo.v2/bson"

type ContractSigningRequest struct {
	Id         bson.ObjectId `json:"-" bson:"_id,omitempty"`
	ContractId string        `json:"contractId"`
	Party      string        `json:"party"`
}
