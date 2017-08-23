package lib0stor

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncrypDecrypt(t *testing.T) {
	f, err := os.Open("/dev/urandom")
	require.NoError(t, err)
	defer f.Close()

	for _, size := range []uint32{
		1 << 10,
		5 << 10,
		10 << 10,
		15 << 10,
		20 << 10,
	} {
		input := make([]byte, 0, size)
		_, err := io.ReadFull(f, input)
		require.NoError(t, err)

		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {

			block, err := Encrypt(bytes.NewReader(input))
			require.NoError(t, err)

			result, err := Decrypt(bytes.NewReader(block.Bytes()), block.cipher)
			require.NoError(t, err)
			assert.Equal(t, input, result)
		})
	}
}

func BenchmarkEncryptMulti(b *testing.B) {
	f, err := os.Open("/dev/urandom")
	require.NoError(b, err)
	defer f.Close()

	inputs := map[uint32][]byte{}

	for _, size := range []uint32{
		1 << 10,
		5 << 10,
		10 << 10,
		15 << 10,
		20 << 10,
	} {
		input := make([]byte, 0, size)
		_, err := io.ReadFull(f, input)
		require.NoError(b, err)
		inputs[size] = input
	}

	b.ResetTimer()
	for size, input := range inputs {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {

			for i := 0; i < b.N; i++ {
				block, err := Encrypt(bytes.NewReader(input))
				require.NoError(b, err)
				_ = block
			}
		})
	}
}
