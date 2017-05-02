package erasure

import "testing"

func BenchmarErasure16_4_4k(b *testing.B) {
	benchmarkErasure(b, 16, 4, 1024*4)
}

func BenchmarkErasure16_4_16k(b *testing.B) {
	benchmarkErasure(b, 16, 4, 1024*16)
}

func benchmarkErasure(b *testing.B, k, m int, length int64) {
	er := NewErasurer(k, m)
	data := make([]byte, length)
	b.SetBytes(length)

	for i := 0; i < b.N; i++ {
		er.Encode("vDiskID", data[:])
	}
}
