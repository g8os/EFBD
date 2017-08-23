package oauthservice

import (
	"crypto/rand"
	"encoding/base64"
)

//Oauth2Client is an oauth2 client
type Oauth2Client struct {
	ClientID                   string
	Label                      string //Label is a just a tag to identity the secret for this ClientID
	Secret                     string
	CallbackURL                string
	ClientCredentialsGrantType bool //ClientCredentialsGrantType indicates if this client can be used in an oauth2 client credentials grant flow
}

//NewOauth2Client creates a new NewOauth2Client with a random secret
func NewOauth2Client(clientID, label, callbackURL string, clientCredentialsGrantType bool) *Oauth2Client {
	c := &Oauth2Client{
		ClientID:                   clientID,
		Label:                      label,
		CallbackURL:                callbackURL,
		ClientCredentialsGrantType: clientCredentialsGrantType,
	}

	randombytes := make([]byte, 39) //Multiple of 3 to make sure no padding is added
	rand.Read(randombytes)
	c.Secret = base64.URLEncoding.EncodeToString(randombytes)
	return c
}
