package encrypt

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	t.Run("aes gcm", func(t *testing.T) {
		privKey := make([]byte, aesGcmKeySize)
		rand.Read(privKey)

		conf := Config{
			Type:    TypeAESGCM,
			PrivKey: string(privKey),
		}
		testRoundTrip(t, conf)
	})
}

func TestNonce(t *testing.T) {
	privKey := make([]byte, aesGcmKeySize)
	plain := []byte("hello world")

	rand.Read(privKey)
	ag, err := newAESGCM(privKey)
	require.NoError(t, err, "cant create encrypter ")

	cipher1, err := ag.Encrypt(plain)
	require.NoError(t, err, "fail to encrypt")
	cipher2, err := ag.Encrypt(plain)
	require.NoError(t, err, "fail to encrypt")

	assert.NotEqual(t, cipher1, cipher2, "2 different call to Encrypt should produce different results")
}

func testRoundTrip(t *testing.T, conf Config) {
	plain := []byte("hello world")

	encdec, err := NewEncrypterDecrypter(conf)
	require.NoError(t, err)

	chiper, err := encdec.Encrypt(plain)
	require.NoError(t, err)

	decrypted, err := encdec.Decrypt(chiper)
	require.NoError(t, err)

	require.Equal(t, plain, decrypted)
}
