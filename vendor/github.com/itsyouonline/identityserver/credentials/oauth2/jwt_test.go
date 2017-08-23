package oauth2

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/assert"
)

const testkey = `-----BEGIN EC PRIVATE KEY-----
MIGkAgEBBDBTgHrh1S5DhiIEMrXW1WCHsYVF4nbr7YS2i1rgxIU7lPEoYrGMgyN8
MPJPUiJwFp+gBwYFK4EEACKhZANiAARIg1wbvTXRTCTVDyKHRM7gRjFVLQc/s3Bj
o2U3ptIJJCD4BVb1nXZlOJ8aIbOKaIrICbpDtU5UtHj9NPSPynb1Q37CFBGy7Du5
CbLpkPStZpzv/j907IQsoqtH6/+SjlY=
-----END EC PRIVATE KEY-----`

//TestGetValidJWT tests the GetValidJWT function
func TestGetValidJWT(t *testing.T) {
	//Setup
	ecdsaKey, _ := jwt.ParseECPrivateKeyFromPEM([]byte(testkey))
	token := jwt.New(jwt.SigningMethodES384)
	token.Claims["username"] = "rob"
	token.Claims["scope"] = "user:email:main"
	token.Claims["azp"] = "example"
	token.Claims["exp"] = time.Now().Unix() * 2
	token.Claims["iss"] = "itsyouonline"

	tokenString, _ := token.SignedString(ecdsaKey)

	r := httptest.NewRequest("", "http://example.com/foo", nil)

	r.Header.Set("Authorization", "bearer "+tokenString)

	j, err := GetValidJWT(r, &ecdsaKey.PublicKey)
	assert.NoError(t, err, "")
	assert.True(t, j.Valid, "Invalid jwt")
}

func TestGetScopesFromJWT(t *testing.T) {
	originaltoken := jwt.New(jwt.SigningMethodHS256)
	originaltoken.Claims["username"] = "rob"
	originaltoken.Claims["scope"] = []string{"1", "2"}
	encodedjwt, _ := originaltoken.SignedString([]byte("abcde"))

	token, err := jwt.Parse(encodedjwt, func(token *jwt.Token) (interface{}, error) {
		return []byte("abcde"), nil
	})

	scopes := GetScopesFromJWT(token)

	assert.NoError(t, err, "")
	assert.EqualValues(t, []string{"1", "2"}, scopes)
}

func TestGetScopestringFromJWT(t *testing.T) {
	originaltoken := jwt.New(jwt.SigningMethodHS256)
	originaltoken.Claims["username"] = "rob"
	originaltoken.Claims["scope"] = []string{"1", "2"}
	encodedjwt, _ := originaltoken.SignedString([]byte("abcde"))

	token, err := jwt.Parse(encodedjwt, func(token *jwt.Token) (interface{}, error) {
		return []byte("abcde"), nil
	})

	scopestring := GetScopestringFromJWT(token)

	assert.NoError(t, err, "")
	assert.Equal(t, "1,2", scopestring, "")
}
