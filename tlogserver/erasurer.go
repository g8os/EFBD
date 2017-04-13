package main

// #include <isa-l/erasure_code.h>
// #include "erasurer.h"
// #cgo LDFLAGS: -lisal
import "C"
import (
	"unsafe"
)

type erasurer struct {
	K         int
	M         int
	encodeTab *C.uchar
}

func newErasurer(k, m int) *erasurer {
	var encodeTab *C.uchar

	C.init_encode_tab(C.int(k), C.int(m), &encodeTab)
	return &erasurer{
		K:         k,
		M:         m,
		encodeTab: encodeTab,
	}
}

func (e *erasurer) encode(data []byte) ([][]byte, error) {
	chunkSize := e.getChunkSize(len(data))
	encodedLen := chunkSize * e.K

	// check if we need to pad the data
	if encodedLen-len(data) > 0 {
		padding := make([]byte, encodedLen-len(data))
		data = append(data, padding...)
	}

	// extend daata buffer to accomodate coded blocks
	{
		codedBlocks := make([]byte, chunkSize*e.M)
		data = append(data, codedBlocks...)
	}

	encodedBlocks := make([][]byte, e.K+e.M)
	encodedBlocksPtr := make([]*byte, e.K+e.M)

	// copy data blocks
	for i := 0; i < e.K; i++ {
		encodedBlocks[i] = data[i*chunkSize : (i+1)*chunkSize]
		encodedBlocksPtr[i] = &encodedBlocks[i][0]
	}
	// copy coding block
	for i := e.K; i < e.K+e.M; i++ {
		encodedBlocks[i] = make([]byte, chunkSize)
		encodedBlocksPtr[i] = &encodedBlocks[i][0]
	}

	C.ec_encode_data(C.int(chunkSize), C.int(e.K), C.int(e.M), unsafe.Pointer(e.encodeTab),
		(**C.uchar)(unsafe.Pointer(&encodedBlocksPtr)), // Pointers to data blocks
		(**C.uchar)(unsafe.Pointer(&encodedBlocksPtr))) // Pointers to parity blocks

	return encodedBlocks, nil
}

func (e *erasurer) getChunkSize(dataLen int) int {
	size := dataLen / e.K
	if dataLen%e.K > 0 {
		size += 1
	}
	return size
}
