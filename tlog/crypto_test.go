package tlog

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	mrand "math/rand"
	"testing"
)

func TestCryptoAES(t *testing.T) {
	encrypter, err := NewAESEncrypter(privTestKey, testNonce)
	if err != nil {
		t.Fatal(err)
	}
	decrypter, err := NewAESDecrypter(privTestKey, testNonce)
	if err != nil {
		t.Fatal(err)
	}

	randTestCase := make([]byte, 4*1024)
	rand.Read(randTestCase)

	testCases := [][]byte{
		make([]byte, 4*1024),
		[]byte("This is a testcase."),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		randTestCase,
	}

	hlrounds := (mrand.Int() % 4) + 3

	for _, original := range testCases {
		// encrypt `hlround` times
		cipher := original
		for i := 0; i < hlrounds; i++ {
			cipher = encrypter.Encrypt(cipher)
		}
		// decrypt `hlround` times
		plain := cipher
		for i := 0; i < hlrounds; i++ {
			plain, err = decrypter.Decrypt(plain)
			if err != nil {
				t.Error(err)
				break
			}
		}
		if err != nil {
			err = nil
			continue
		}

		if bytes.Compare(original, plain) != 0 {
			t.Errorf(
				"plaintext expected to be %v, while received %v",
				original, plain)
		}
	}
}

func BenchmarkAES_4k(b *testing.B) {
	benchmarkAES(b, 4*1024)
}

func BenchmarkAES_8k(b *testing.B) {
	benchmarkAES(b, 8*1024)
}

func BenchmarkAES_32k(b *testing.B) {
	benchmarkAES(b, 32*1024)
}

func benchmarkAES(b *testing.B, size int64) {
	encrypter, err := NewAESEncrypter(privTestKey, testNonce)
	if err != nil {
		b.Fatal(err)
	}
	decrypter, err := NewAESDecrypter(privTestKey, testNonce)
	if err != nil {
		b.Fatal(err)
	}

	in := make([]byte, size)
	b.SetBytes(size)

	var out []byte
	var result []byte
	var prevOut []byte

	for i := 0; i < b.N; i++ {
		out = encrypter.Encrypt(in)
		if prevOut != nil && bytes.Compare(prevOut, out) != 0 {
			b.Errorf(
				"encrypted package was expected to be %v, while received %v",
				prevOut, out)
			continue
		}

		result, err = decrypter.Decrypt(out)
		if err != nil {
			b.Error(err)
			continue
		}

		if bytes.Compare(in, result) != 0 {
			b.Errorf(
				"decrypted package was expected to be %v, while received %v",
				in, result)
			continue
		}

		prevOut = out
	}
}

var (
	privTestKey string
	testNonce   string
)

func init() {
	privKey := make([]byte, KeySize)
	rand.Read(privKey)
	privTestKey = string(privKey)

	nonce := make([]byte, NonceSize)
	rand.Read(nonce)
	testNonce = hex.EncodeToString(nonce)
}
