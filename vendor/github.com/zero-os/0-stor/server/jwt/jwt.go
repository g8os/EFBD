package jwt

import (
	"crypto"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	jwtgo "github.com/dgrijalva/jwt-go"
)

const (
	iyoPublicKeyStr = `-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAES5X8XrfKdx9gYayFITc89wad4usrk0n2
7MjiGYvqalizeSWTHEpnd7oea9IQ8T5oJjMVH5cc0H5tFSKilFFeh//wngxIyny6
6+Vq5t5B0V0Ehy01+2ceEon2Y0XDkIKv
-----END PUBLIC KEY-----`
)

var iyoPublicKey crypto.PublicKey

func init() {
	var err error
	iyoPublicKey, err = jwtgo.ParseECPublicKeyFromPEM([]byte(iyoPublicKeyStr))
	if err != nil {
		log.Fatalf("failed to parse pub key:%v", err)
	}

}

// CheckPermissions checks whether user has needed scopes
func CheckPermissions(expectedScopes, userScopes []string) bool {
	for _, scope := range userScopes {
		scope = strings.Replace(scope, "user:memberof:", "", 1)
		for _, expected := range expectedScopes {
			if scope == expected {
				return true
			}
		}
	}
	return false
}

// ValidateNamespaceLabel check if a label follow the right pattern
func ValidateNamespaceLabel(nsid string) error {

	// subOrg_0stor_org i.e first_0stor_gig
	if strings.Count(nsid, "_0stor_") != 1 || strings.HasSuffix(nsid, "_0stor_") {
		err := fmt.Errorf("Invalid namespace label: %s", nsid)
		log.Error(err.Error())
		return err
	}

	return nil
}

// check JWT token and get it's scopes
func CheckJWTGetScopes(tokenStr string) ([]string, int64, error) {
	jwtStr := strings.TrimSpace(strings.TrimPrefix(tokenStr, "Bearer"))

	token, err := jwtgo.Parse(jwtStr, func(token *jwtgo.Token) (interface{}, error) {
		if token.Method != jwtgo.SigningMethodES384 {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return iyoPublicKey, nil
	})
	if err != nil {
		return nil, 0, err
	}

	claims, ok := token.Claims.(jwtgo.MapClaims)
	if !(ok && token.Valid) {
		return nil, 0, fmt.Errorf("invalid token")
	}

	var scopes []string
	for _, v := range claims["scope"].([]interface{}) {
		scopes = append(scopes, v.(string))
	}

	exp, ok := claims["exp"].(float64)
	if !ok {
		return nil, 0, fmt.Errorf("invalid expiration claims in token")
	}
	return scopes, int64(exp), nil
}
