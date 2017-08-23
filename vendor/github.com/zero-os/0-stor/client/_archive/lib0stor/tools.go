package lib0stor

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
)

func hash(r io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func compress(r io.Reader) ([]byte, error) {
	compressed := bytes.Buffer{}

	w := snappy.NewBufferedWriter(&compressed)
	if _, err := io.Copy(w, r); err != nil {
		return nil, err
	}
	w.Close()
	return compressed.Bytes(), nil
}

func decompress(r io.Reader) ([]byte, error) {
	decompressed := bytes.Buffer{}

	sr := snappy.NewReader(r)
	if _, err := io.Copy(&decompressed, sr); err != nil {
		return nil, err
	}

	return decompressed.Bytes(), nil
}

func encrypt(plaintext []byte, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decrypt(ciphertext []byte, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func crc(data []byte) uint32 {
	h := crc32.NewIEEE()
	return h.Sum32()
}
