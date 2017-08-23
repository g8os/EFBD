package oauthservice

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAccessTokenExpiration(t *testing.T) {
	ar := &AccessToken{CreatedAt: time.Now()}

	assert.True(t, ar.IsExpiredAt(ar.CreatedAt.Add(AccessTokenExpiration).Add(time.Second)))
	assert.False(t, ar.IsExpiredAt(ar.CreatedAt.Add(AccessTokenExpiration)))
}

func TestNewAccessToken(t *testing.T) {
	at := newAccessToken("user1", "globalid1", "client1", "scope")
	assert.NotEmpty(t, at.AccessToken)
	assert.False(t, strings.HasSuffix(at.AccessToken, "="))
	assert.NotEqual(t, time.Time{}, at.CreatedAt)
	assert.Equal(t, "user1", at.Username)
	assert.Equal(t, "client1", at.ClientID)
	assert.Equal(t, "globalid1", at.GlobalID)
	assert.Equal(t, "scope", at.Scope)
}
