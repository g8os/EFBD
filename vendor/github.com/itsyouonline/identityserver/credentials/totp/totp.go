package totp

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base32"
	"fmt"

	"github.com/hgfischer/go-otp"
	"net/http"
	"strings"
)

const tokenLength = sha1.Size
const provider = "ItsYou.Online"

//Token represents a totp token with a base32 encoded secret
type Token struct {
	Provider string
	User     string
	Secret   string
}

//NewToken creates a new totp token with a random base32 encoded secret
func NewToken() (*Token, error) {
	byteSecret, err := generateRandomBytes(tokenLength)
	secret := base32.StdEncoding.EncodeToString(byteSecret)
	return &Token{
		Provider: provider,
		User:     "",
		Secret:   secret,
	}, err
}

//TokenFromSecret creates a totp token from an existing base32 encoded secret
func TokenFromSecret(secret string) *Token {
	return &Token{
		Provider: provider,
		User:     "",
		Secret:   secret,
	}
}

//Validate checks a securityCode against a totp token
func (token *Token) Validate(securityCode string) (valid bool) {

	totp := otp.TOTP{
		Secret:         token.Secret,
		IsBase32Secret: true,
	}
	valid = totp.Now().Verify(securityCode)
	return

}

//URL creates a the Key Uri Format url (otpauth://... )to enable users to pick this up in the totp applications
func (token *Token) URL() string {

	return fmt.Sprintf(
		"otpauth://totp/%s:%s?secret=%s&issuer=%s",
		token.Provider,
		token.User,
		token.Secret,
		token.Provider,
	)
}

func generateRandomBytes(length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return b, err
	}
	return b, nil
}

func GetIssuer(r *http.Request) string {
	totpIssuer := "It's You Online"
	if strings.Contains(r.Host, "staging") {
		totpIssuer += " staging"
	} else if strings.Contains(r.Host, "dev") {
		totpIssuer += " dev"
	}
	return totpIssuer
}
