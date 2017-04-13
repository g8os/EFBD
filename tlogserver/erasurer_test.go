package main

import (
	"testing"

	"github.com/templexxx/reedsolomon"
)

func BenchmarkReedsolomonErasure16_4_4k(b *testing.B) {
	benchmarkReedsolomonErasure(b, 16, 4, 1024*4)
}

func BenchmarkReedsolomonErasure16_4_16k(b *testing.B) {
	benchmarkReedsolomonErasure(b, 16, 4, 1024*16)
}

func BenchmarkIsalErasure16_4_4k(b *testing.B) {
	benchmarkIsalErasure(b, 16, 4, 1024*4)
}

func BenchmarkIsalErasure16_4_16k(b *testing.B) {
	benchmarkIsalErasure(b, 16, 4, 1024*16)
}

func benchmarkIsalErasure(b *testing.B, k, m int, length int) {
	er := newErasurer(k, m)
	data := make([]byte, length)

	b.SetBytes(int64(length))
	for i := 0; i < b.N; i++ {
		er.encodeIsal(data[:])
	}
}

func benchmarkReedsolomonErasure(b *testing.B, k, m int, length int) {
	er := newErasurer(k, m)
	enc, _ := reedsolomon.New(k, m)
	data := make([]byte, length)

	b.SetBytes(int64(length))
	for i := 0; i < b.N; i++ {
		er.encodeTemplex(enc, data[:])
	}
}
