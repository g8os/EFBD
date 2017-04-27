package tlog

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
)

// static sizes
const (
	KeySize   = 32 // 256-bit key
	NonceSize = 12 // 96-bit nonce
)

// NewAESEncrypter creates a new encrypter,
// using the given private key and nonce,
// ready to encrypt your plaintext
func NewAESEncrypter(privKey, hexNonce string) (encrypter AESEncrypter, err error) {
	nonce, err := validateInput(privKey, hexNonce)
	if err != nil {
		return
	}

	encrypter, err = newSTDStreamCipher([]byte(privKey), nonce)
	return
}

// NewAESDecrypter creates a new decrypter,
// using the given private key and nonce,
// ready to decrypt your ciphertext (that was encrypted by the AESEncrypter)
func NewAESDecrypter(privKey, hexNonce string) (decrypter AESDecrypter, err error) {
	nonce, err := validateInput(privKey, hexNonce)
	if err != nil {
		return
	}

	decrypter, err = newSTDStreamCipher([]byte(privKey), nonce)
	return
}

// AESEncrypter encrypts plaintext into ciphertext,
// and can be decrypted by using the Decrypter
type AESEncrypter interface {
	Encrypt(plain []byte) (cipher []byte)
}

// AESDecrypter decrypts ciphertext,
// encrypted by the AESEncrypter, into plaintext.
type AESDecrypter interface {
	Decrypt(cipher []byte) (plan []byte, err error)
}

func newSTDStreamCipher(privKey, nonce []byte) (stream *stdStreamCipher, err error) {
	block, err := aes.NewCipher(privKey)
	if err != nil {
		return // unexpected, shouldn't happen (see: crypto.go validation)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return // unexpected, shouldn't happen (see: crypto.go validation)
	}

	stream = &stdStreamCipher{aesgcm, nonce}
	return
}

type stdStreamCipher struct {
	aesgcm cipher.AEAD
	nonce  []byte
}

// Encrypt implements AESEncrypter.Encrypt
func (s *stdStreamCipher) Encrypt(plain []byte) (cipher []byte) {
	cipher = s.aesgcm.Seal(nil, s.nonce, plain, nil)
	return
}

// Decrypt implements AESDecrypter.Decrypt
func (s *stdStreamCipher) Decrypt(cipher []byte) (plain []byte, err error) {
	plain, err = s.aesgcm.Open(nil, s.nonce, cipher, nil)
	return
}

func validateInput(privKey, hexNonce string) (nonce []byte, err error) {
	if len(privKey) != KeySize {
		err = errors.New("invalid keysize")
		return
	}

	nonce, err = hex.DecodeString(hexNonce)
	if err != nil {
		err = errors.New("invalid nonce, needs to be hex")
		return
	}

	if len(nonce) != NonceSize {
		nonce = nil
		err = errors.New("invalid noncesize")
		return
	}

	return
}
