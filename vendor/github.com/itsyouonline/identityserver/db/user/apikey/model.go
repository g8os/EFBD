package apikey

import (
	"crypto/rand"
	"encoding/base64"
	"gopkg.in/mgo.v2/bson"
)

type APIKey struct {
	ID            bson.ObjectId `json:"-" bson:"_id,omitempty"`
	Label         string        `json:"label"`
	Scopes        []string      `json:"scopes"`
	ApplicationID string        `json:"applicationid"`
	ApiKey        string        `json:"apikey"`
	Username      string        `json:"username"`
}

func NewAPIKey(username string, label string) *APIKey {
	var apikey APIKey
	randombytes := make([]byte, 21) //Multiple of 3 to make sure no padding is added
	rand.Read(randombytes)
	apikey.ApiKey = base64.URLEncoding.EncodeToString(randombytes)
	randombytes = make([]byte, 21) //Multiple of 3 to make sure no padding is added
	rand.Read(randombytes)
	apikey.ApplicationID = base64.URLEncoding.EncodeToString(randombytes)
	apikey.Username = username
	apikey.Label = label
	apikey.Scopes = []string{"user:admin"}

	return &apikey
}
