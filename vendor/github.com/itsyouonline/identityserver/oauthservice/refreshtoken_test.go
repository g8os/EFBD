package oauthservice

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRefreshToken(t *testing.T) {
	a := newRefreshToken()
	assert.NotEmpty(t, a.RefreshToken)
	assert.False(t, strings.HasSuffix(a.RefreshToken, "="))
}
