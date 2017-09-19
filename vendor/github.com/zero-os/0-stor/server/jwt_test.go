package server

import (
	"io/ioutil"
	"testing"

	jwtgo "github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/server/jwt"
	"github.com/zero-os/0-stor/stubs"
)

var (
	token string
)

func getToken(t testing.TB) string {
	b, err := ioutil.ReadFile("../devcert/jwt_key.pem")
	require.NoError(t, err)

	key, err := jwtgo.ParseECPrivateKeyFromPEM(b)
	require.NoError(t, err)

	iyoCl, err := stubs.NewStubIYOClient("testorg", key)

	token, err = iyoCl.CreateJWT("mynamespace", itsyouonline.Permission{
		Read:   true,
		Write:  true,
		Delete: true,
	})
	if err != nil {
		t.Fatal("failed to create iyo token:" + err.Error())
	}

	return token
}

func BenchmarkJWTCache(b *testing.B) {
	token := getToken(b)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := getScopes(token)
		if err != nil {
			b.Fatalf("getScopes failed:%v", err)
		}
	}
}

func BenchmarkJWTWithoutCache(b *testing.B) {
	token := getToken(b)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _, err := jwt.CheckJWTGetScopes(token)
		if err != nil {
			b.Fatalf("getScopes failed:%v", err)
		}
	}
}
