package distribution

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeRoundTrip(t *testing.T) {
	k := 4

	tests := []struct {
		name    string
		dataLen int
	}{
		{"no need for padding ", padFactor * k},
		{"need padding", (padFactor * 30) - 1},
		{"need padding", (padFactor * (k + 1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testEncodeRoundTrip(t, k, test.dataLen)
		})
	}
}

func testEncodeRoundTrip(t *testing.T, k, dataLen int) {
	const (
		m = 2
	)

	// encode data
	data := make([]byte, dataLen)
	rand.Read(data)

	e, err := NewEncoder(k, m)
	assert.Nil(t, err)

	encoded, err := e.Encode(data)
	assert.Nil(t, err)

	// decode
	decodedChunks := make([][]byte, k+m)
	for i := 0; i < k+m; i++ {
		if i < m {
			// simulate losing pieces here
			// we can lost up to `m` pieces
			continue
		}
		decodedChunks[i] = make([]byte, len(encoded[i]))
		copy(decodedChunks[i], encoded[i])
	}

	dec, err := NewDecoder(k, m)
	assert.Nil(t, err)
	decoded, err := dec.Decode(decodedChunks, len(data))
	assert.Nil(t, err)

	if !assert.Equal(t, data, decoded) {
		return
	}
}
