// +build isal

package erasure

// #include <isa-l/erasure_code.h>
// #include "erasurer_isal.h"
// #cgo LDFLAGS: -lisal
import "C"
import (
	"unsafe"
)

type isalErasurer struct {
	K         int
	M         int
	encodeTab []byte // isa-l encode table
}

func newIsalErasurer(k, m int) EraruseCoder {
	encodeTab := make([]byte, 32*k*(k+m))

	C.init_encode_tab(C.int(k), C.int(m), (*C.uchar)(unsafe.Pointer(&encodeTab[0])))
	return &isalErasurer{
		K:         k,
		M:         m,
		encodeTab: encodeTab,
	}
}

func (e *isalErasurer) Encode(volID string, data []byte) ([][]byte, error) {
	return e.encodeIsal(data)
}

func (e *isalErasurer) encodeIsal(data []byte) ([][]byte, error) {
	chunkSize := e.getChunkSize(len(data))

	encoded := e.allocateEncodedBlocks(data[:])
	ptrs := make([]*byte, e.K+e.M)

	// create pointers  blocks
	for i := 0; i < e.K+e.M; i++ {
		ptrs[i] = &encoded[i][0]
	}

	C.ec_encode_data(C.int(chunkSize), C.int(e.K), C.int(e.M),
		(*C.uchar)(unsafe.Pointer(&e.encodeTab[0])),
		(**C.uchar)(unsafe.Pointer(&ptrs[:e.K][0])), // Pointers to data blocks
		(**C.uchar)(unsafe.Pointer(&ptrs[e.K:][0]))) // Pointers to parity blocks

	return encoded, nil
}

func (e *isalErasurer) allocateEncodedBlocks(data []byte) [][]byte {
	chunkSize := e.getChunkSize(len(data))

	encoded := make([][]byte, e.K+e.M)
	encodedLen := chunkSize * e.K

	// check if we need to pad the data
	if encodedLen-len(data) > 0 {
		padding := make([]byte, encodedLen-len(data))
		data = append(data, padding...)
	}

	// copy data blocks
	for i := 0; i < e.K; i++ {
		encoded[i] = data[i*chunkSize : (i+1)*chunkSize]
	}

	// allocate coding block
	for i := e.K; i < e.K+e.M; i++ {
		encoded[i] = make([]byte, chunkSize)
	}
	return encoded
}

func (e *isalErasurer) getChunkSize(dataLen int) int {
	size := dataLen / e.K
	if dataLen%e.K > 0 {
		size++
	}
	return size
}

func init() {
	NewErasuser = newIsalErasurer
}
