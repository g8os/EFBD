package erasure

import "testing"

func BenchmarkReedsolomonErasure16_4_4k(b *testing.B) {
	benchmarkReedsolomonErasure(b, 16, 4, 1024*4)
}

func BenchmarkReedsolomonErasure16_4_16k(b *testing.B) {
	benchmarkReedsolomonErasure(b, 16, 4, 1024*16)
}

func benchmarkReedsolomonErasure(b *testing.B, k, m int, length int) {
	er := newErasurer(k, m)
	data := make([]byte, length)

	for i := 0; i < b.N; i++ {
		er.Encode("vDiskID", data[:])
	}
}
