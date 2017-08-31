package grpc

import (
	"os"
	"testing"

	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/server/jwt"
)

var (
	token string
)

func init() {
	var err error
	org := os.Getenv("iyo_organization")
	clientID := os.Getenv("iyo_client_id")
	clientSecret := os.Getenv("iyo_secret")
	iyoClient := itsyouonline.NewClient(org, clientID, clientSecret)

	token, err = iyoClient.CreateJWT("mynamespace", itsyouonline.Permission{
		Read:   true,
		Write:  true,
		Delete: true,
	})
	if err != nil {
		panic("failed to create iyo token:" + err.Error())
	}
}

func BenchmarkJWTCache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := getScopes(token)
		if err != nil {
			b.Fatalf("getScopes failed:%v", err)
		}
	}
}

func BenchmarkJWTWithoutCache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, _, err := jwt.CheckJWTGetScopes(token)
		if err != nil {
			b.Fatalf("getScopes failed:%v", err)
		}
	}
}
