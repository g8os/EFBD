package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// Key and nonce size
const (
	aesGcmNonceSize = 12
	aesGcmKeySize   = 32
)

type aesgcm struct {
	w   io.Writer
	gcm cipher.AEAD
}

func newAESGCM(privKey []byte) (*aesgcm, error) {
	if len(privKey) != aesGcmKeySize {
		return nil, errInvalidPrivKeyLen
	}

	block, err := aes.NewCipher(privKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return &aesgcm{
		gcm: gcm,
	}, nil
}

func (ag *aesgcm) Encrypt(plain []byte) ([]byte, error) {
	nonce := make([]byte, ag.gcm.NonceSize())
	_, err := io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, err
	}

	return ag.gcm.Seal(nonce, nonce, plain, nil), nil
}

func (ag *aesgcm) Decrypt(cipher []byte) ([]byte, error) {
	if len(cipher) < ag.gcm.NonceSize() {
		return nil, errors.New("malformed ciphertext")
	}

	return ag.gcm.Open(nil, cipher[:ag.gcm.NonceSize()], cipher[ag.gcm.NonceSize():], nil)
}
