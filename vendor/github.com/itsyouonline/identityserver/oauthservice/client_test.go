package oauthservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewOauth2Client(t *testing.T) {
	c := NewOauth2Client("client1", "main", "http://www.callback.org", false)
	assert.Equal(t, "client1", c.ClientID)
	assert.Equal(t, "main", c.Label)
	assert.Equal(t, "http://www.callback.org", c.CallbackURL)
	assert.Equal(t, false, c.ClientCredentialsGrantType)
	assert.NotEmpty(t, c.Secret)

	c2 := NewOauth2Client("clientid", "", "", true)
	assert.NotEqual(t, c.Secret, c2.Secret)
}
