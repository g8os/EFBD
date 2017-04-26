// +build isal

package erasure

import (
	"testing"
)

func BenchmarkIsalErasure16_4_4k(b *testing.B) {
	benchmarkIsalErasure(b, 16, 4, 1024*4)
}

func BenchmarkIsalErasure16_4_16k(b *testing.B) {
	benchmarkIsalErasure(b, 16, 4, 1024*16)
}

func benchmarkIsalErasure(b *testing.B, k, m int, length int) {
	er := newIsalErasurer(k, m)
	data := make([]byte, length)

	b.SetBytes(int64(length))
	for i := 0; i < b.N; i++ {
		er.Encode("vDiskID", data[:])
	}
}
