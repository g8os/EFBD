package encrypt

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
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

	// encrypt
	buf := block.NewBytesBuffer()

	w, err := NewWriter(buf, conf)
	require.Nil(t, err)

	md, err := meta.New(nil, 0, nil)
	require.Nil(t, err)

	_, err = w.WriteBlock(nil, plain, md)
	require.Nil(t, err)

	// decrypt ag
	ag, err := newAESGCM([]byte(conf.PrivKey))
	require.Nil(t, err)

	dag, err := ag.Decrypt(buf.Bytes())
	require.Nil(t, err)
	require.Equal(t, plain, dag)

	// decrypt
	r, err := NewReader(conf)
	require.Nil(t, err)

	decrypted, err := r.ReadBlock(buf.Bytes())
	require.Nil(t, err)

	require.Equal(t, plain, decrypted)
}
