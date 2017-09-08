package stubs

import (
	"crypto"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/zero-os/0-stor/client/itsyouonline"
)

type IYOClient interface {
	CreateJWT(namespace string, perm itsyouonline.Permission) (string, error)
	CreateNamespace(namespace string) error
	DeleteNamespace(namespace string) error
	GivePermission(namespace, userID string, perm itsyouonline.Permission) error
	RemovePermission(namespace, userID string, perm itsyouonline.Permission) error
	GetPermission(namespace, userID string) (itsyouonline.Permission, error)
}

// StubIYOClient implements the IYOClient interface
// this intended to be use for unit testing
type StubIYOClient struct {
	organization  string
	jwtSingingKey crypto.PrivateKey
}

func NewStubIYOClient(organization string, key crypto.PrivateKey) (IYOClient, error) {
	return &StubIYOClient{
		organization:  organization,
		jwtSingingKey: key,
	}, nil
}

// CreateJWT generate a JWT that can be used for testing
func (m *StubIYOClient) CreateJWT(namespace string, perm itsyouonline.Permission) (string, error) {
	claims := jwt.MapClaims{
		"exp":   time.Now().Add(time.Hour * 24).Unix(),
		"scope": perm.Scopes(m.organization, "0stor."+namespace),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES384, claims)
	return token.SignedString(m.jwtSingingKey)
}

func (m *StubIYOClient) CreateNamespace(namespace string) error {
	return nil
}
func (m *StubIYOClient) DeleteNamespace(namespace string) error {
	return nil
}
func (m *StubIYOClient) GivePermission(namespace, userID string, perm itsyouonline.Permission) error {
	return nil
}
func (m *StubIYOClient) RemovePermission(namespace, userID string, perm itsyouonline.Permission) error {
	return nil
}
func (m *StubIYOClient) GetPermission(namespace, userID string) (itsyouonline.Permission, error) {
	return itsyouonline.Permission{
		Admin:  true,
		Write:  true,
		Read:   true,
		Delete: true,
	}, nil
}
