package backup

import (
	"bytes"
	"crypto/rand"
	mrand "math/rand"
	"sync"
	"testing"

	"github.com/zero-os/0-Disk/testdata"
)

func TestCompressLZ4(t *testing.T) {
	compressor := LZ4Compressor()
	decompressor := LZ4Decompressor()

	testCompress(t, compressor, decompressor)
}

func TestCompressXZ(t *testing.T) {
	compressor := XZCompressor()
	decompressor := XZDecompressor()

	testCompress(t, compressor, decompressor)
}

func testCompress(t *testing.T, compressor Compressor, decompressor Decompressor) {
	randTestCase := make([]byte, 4*1024)
	rand.Read(randTestCase)

	testCases := [][]byte{
		make([]byte, 4*1024),
		[]byte("This is a testcase."),
		[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		randTestCase,
	}

	hlrounds := (mrand.Int() % 4) + 3

	var bufA, bufB bytes.Buffer

	for _, original := range testCases {
		var err error

		// encrypt `hlround` times
		bufA.Reset()
		_, err = bufA.Write(original)
		if err != nil {
			t.Error(err)
			break
		}
		for i := 0; i < hlrounds; i++ {
			err = compressor.Compress(&bufA, &bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufA.Reset()
			_, err = bufA.ReadFrom(&bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufB.Reset()
		}
		if err != nil {
			err = nil
			continue
		}
		// decrypt `hlround` times
		for i := 0; i < hlrounds; i++ {
			err = decompressor.Decompress(&bufA, &bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufA.Reset()
			_, err = bufA.ReadFrom(&bufB)
			if err != nil {
				t.Error(err)
				break
			}
			bufB.Reset()
		}
		if err != nil {
			err = nil
			continue
		}

		plain := bufA.Bytes()

		if bytes.Compare(original, plain) != 0 {
			t.Errorf(
				"plaintext expected to be %v, while received %v",
				original, plain)
		}
	}
}

func BenchmarkLZ4_4k(b *testing.B) {
	benchmarkLZ4(b, 4*1024)
}

func BenchmarkLZ4_8k(b *testing.B) {
	benchmarkLZ4(b, 8*1024)
}

func BenchmarkLZ4_16k(b *testing.B) {
	benchmarkLZ4(b, 16*1024)
}

func BenchmarkLZ4_32k(b *testing.B) {
	benchmarkLZ4(b, 32*1024)
}

func benchmarkLZ4(b *testing.B, size int64) {
	compressor := LZ4Compressor()
	decompressor := LZ4Decompressor()

	benchmarkCompressor(b, size, compressor, decompressor)
}

func BenchmarkXZ_4k(b *testing.B) {
	benchmarkXZ(b, 4*1024)
}

func BenchmarkXZ_8k(b *testing.B) {
	benchmarkXZ(b, 8*1024)
}

func BenchmarkXZ_16k(b *testing.B) {
	benchmarkXZ(b, 16*1024)
}

func BenchmarkXZ_32k(b *testing.B) {
	benchmarkXZ(b, 32*1024)
}

func benchmarkXZ(b *testing.B, size int64) {
	compressor := XZCompressor()
	decompressor := XZDecompressor()

	benchmarkCompressor(b, size, compressor, decompressor)
}

func benchmarkCompressor(b *testing.B, size int64, compressor Compressor, decompressor Decompressor) {
	in := generateCompressBenchData(b, size)

	var cbytes []byte
	var logCompressionRatioOnce sync.Once

	logCompressionRatioOnceBody := func() {
		dlen := int64(len(cbytes))
		b.Logf("compressed %d bytes down to %d bytes (~%f times smaller)",
			size, dlen, float64(size)/float64(dlen))
	}

	for i := 0; i < b.N; i++ {
		var err error
		var bufA, bufB bytes.Buffer

		_, err = bufA.Write(in)
		if err != nil {
			b.Error(err)
			continue
		}

		err = compressor.Compress(&bufA, &bufB)
		if err != nil {
			b.Error(err)
			continue
		}

		cbytes = bufB.Bytes()
		logCompressionRatioOnce.Do(logCompressionRatioOnceBody)

		bufA.Reset()
		err = decompressor.Decompress(&bufB, &bufA)
		if err != nil {
			b.Error(err)
			continue
		}

		result := bufA.Bytes()
		if bytes.Compare(in, result) != 0 {
			b.Errorf(
				"decompressed package was expected to be %v, while received %v",
				in, result)
			continue
		}
	}
}

func generateCompressBenchData(b *testing.B, size int64) []byte {
	b.StopTimer()
	defer b.StartTimer()
	b.SetBytes(size)

	ibm := getLedeImageBlocks()
	benchData := make([]byte, size)

	offset := int64(0)
	for _, data := range ibm {
		copy(benchData[offset:], data)
		offset += testdata.LedeImageBlockSize
		if offset >= size {
			break
		}
	}

	return benchData
}
