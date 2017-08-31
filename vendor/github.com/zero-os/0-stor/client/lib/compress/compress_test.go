package compress

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// TestRoundTrip tests that compress and uncompress is the identity
// function.
func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		typ  string
	}{
		{"gzip", TypeGzip},
		{"snappy", TypeSnappy},
		{"lz4", TypeLz4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testRoundTrip(t, Config{
				Type: test.typ,
			})
		})
	}
}

func testRoundTrip(t *testing.T, conf Config) {
	payload := make([]byte, 4096*4096)
	for i := 0; i < len(payload); i++ {
		payload[i] = 100
	}

	buf := block.NewBytesBuffer()

	// create writer
	w, err := NewWriter(conf, buf)
	assert.NoError(t, err)

	md := meta.New(nil)
	// compress by write to the writer
	_, err = w.WriteBlock(nil, payload, md)
	assert.NoError(t, err)

	// create reader
	r, err := NewReader(conf)
	if !assert.NoError(t, err) {
		return
	}

	// decompress by read from the reader
	b, err := r.ReadBlock(buf.Bytes())
	if !assert.NoError(t, err) {
		return
	}

	// compare decompressed with original payload
	if bytes.Compare(payload, b) != 0 {
		t.Fatalf("decompressed(%v) != data(%v)", len(b), len(payload))
	}
}
