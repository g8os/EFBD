package encrypt

import (
	"errors"
	"fmt"
)

// Encryption type
const (
	TypeAESGCM = "aes_gcm"
)

var (
	errInvalidPrivKeyLen = errors.New("invalid private key length")
	errInvalidNonceLen   = errors.New("invalid nonce length")
)

// Config defines EncrypterDecrypter config
type Config struct {
	Type    string `yaml:"type"`
	PrivKey string `yaml:"privKey"`
}

// EncrypterDecrypter is interaface for encrypter and decrypter
type EncrypterDecrypter interface {
	Encrypt(plain []byte) ([]byte, error)
	Decrypt(cipher []byte) (plain []byte, err error)
}

// NewEncrypterDecrypter creates new EncrypterDecrypter
func NewEncrypterDecrypter(conf Config) (EncrypterDecrypter, error) {
	switch conf.Type {
	case TypeAESGCM:
		return newAESGCM([]byte(conf.PrivKey))
	default:
		return nil, fmt.Errorf("invalid type: %v", conf.Type)
	}
}
