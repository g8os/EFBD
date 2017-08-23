package totp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTOTP(t *testing.T) {
	token, err := NewToken()
	assert.NoError(t, err)
	assert.NotNil(t, token)
	assert.NotEmpty(t, token.Secret)

	fromSecret := TokenFromSecret(token.Secret)
	assert.NotNil(t, fromSecret)
	assert.NotEmpty(t, fromSecret.Secret)
}
