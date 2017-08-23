package oauthservice

import (
	"encoding/base64"
	"github.com/itsyouonline/identityserver/db"
	"math/rand"
	"time"
)

type refreshToken struct {
	RefreshToken string
	//Parent refers to another authorization's RefreshToken
	Parent          string
	Scopes          []string
	Expires         *time.Time
	LastUsed        db.DateTime
	Subject         string
	AuthorizedParty string
}

func newRefreshToken() (auth refreshToken) {
	randombytes := make([]byte, 21) //Multiple of 3 to make sure no padding is added
	rand.Read(randombytes)
	auth.RefreshToken = base64.URLEncoding.EncodeToString(randombytes)
	return
}
