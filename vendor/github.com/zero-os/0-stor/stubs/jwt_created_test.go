package stubs

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-stor/client/itsyouonline"
)

func TestCreateJWT(t *testing.T) {
	pubKey, err := ioutil.ReadFile("../devcert/jwt_pub.pem")
	require.NoError(t, err)

	b, err := ioutil.ReadFile("../devcert/jwt_key.pem")
	require.NoError(t, err)

	key, err := jwt.ParseECPrivateKeyFromPEM(b)
	require.NoError(t, err)

	iyoCl, err := NewStubIYOClient("testorg", key)
	assert.NoError(t, err)

	tokenString, err := iyoCl.CreateJWT("testns", itsyouonline.Permission{
		Admin:  true,
		Read:   true,
		Write:  true,
		Delete: true,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, tokenString)

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return pubKey, nil
	})

	claims, ok := token.Claims.(jwt.MapClaims)
	require.True(t, ok, "bad claims format")

	var scopes []string
	for _, v := range claims["scope"].([]interface{}) {
		scopes = append(scopes, v.(string))
	}
	assert.Contains(t, scopes, "user:memberof:testorg.0stor.testns.read")
	assert.Contains(t, scopes, "user:memberof:testorg.0stor.testns.write")
	assert.Contains(t, scopes, "user:memberof:testorg.0stor.testns.delete")
	assert.Contains(t, scopes, "user:memberof:testorg.0stor.testns")
}
