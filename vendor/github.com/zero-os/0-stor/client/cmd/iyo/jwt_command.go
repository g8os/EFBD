package main

import (
	"crypto/ecdsa"
	"fmt"
	"log"

	"github.com/dgrijalva/jwt-go"
	"github.com/zero-os/0-stor/client/itsyouonline"
)

// JWTPublicKey is JWT public key of the server
var JWTPublicKey *ecdsa.PublicKey

const (
	oauth2ServerPublicKey = `-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAES5X8XrfKdx9gYayFITc89wad4usrk0n2
7MjiGYvqalizeSWTHEpnd7oea9IQ8T5oJjMVH5cc0H5tFSKilFFeh//wngxIyny6
6+Vq5t5B0V0Ehy01+2ceEon2Y0XDkIKv
-----END PUBLIC KEY-----` // fill it with oauth2 server public key
)

func init() {
	var err error

	if len(oauth2ServerPublicKey) == 0 {
		return
	}
	JWTPublicKey, err = jwt.ParseECPublicKeyFromPEM([]byte(oauth2ServerPublicKey))
	if err != nil {
		log.Fatalf("failed to parse pub key:%v", err)
	}

}

type jwtCommand struct {
	clientID  string
	secret    string
	org       string
	namespace string
	read      bool
	write     bool
	delete    bool
}

func (jc jwtCommand) Execute() error {
	log.Println("hello")
	log.Printf("client_id=%v\n,secret=%v\n,org=%v\n,ns=%v\n,read=%v\n,write=%v\n,delete=%v\n",
		jc.clientID,
		jc.secret,
		jc.org,
		jc.namespace,
		jc.read,
		jc.write,
		jc.delete)

	c := itsyouonline.NewClient(jc.org, jc.clientID, jc.secret)

	token, err := c.CreateJWT(jc.namespace, itsyouonline.Permission{
		Read:   jc.read,
		Write:  jc.write,
		Delete: jc.delete,
	})
	if err != nil {
		log.Fatalf("failed to create token:%v", err)
	}
	log.Printf("token = %v\n", token)
	scopes, err := jc.getScopes(token)
	log.Printf("scopes=%v, err=%v\n", scopes, err)
	return nil
}

func (jc jwtCommand) getScopes(tokenStr string) ([]string, error) {
	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodES384 {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return JWTPublicKey, nil
	})
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !(ok && token.Valid) {
		return nil, fmt.Errorf("invalid token")
	}

	var scopes []string
	for _, v := range claims["scope"].([]interface{}) {
		scopes = append(scopes, v.(string))
	}
	return scopes, nil
}
