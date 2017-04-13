package main

// #include <isa-l/erasure_code.h>
// #include "erasurer.h"
// #cgo LDFLAGS: -lisal
import "C"
import (
	"unsafe"

	"github.com/klauspost/reedsolomon"
)

type erasurer struct {
	K         int
	M         int
	encodeTab []byte
}

func newErasurer(k, m int) *erasurer {
	encodeTab := make([]byte, 32*k*(k+m))

	C.init_encode_tab(C.int(k), C.int(m), (*C.uchar)(unsafe.Pointer(&encodeTab[0])))
	return &erasurer{
		K:         k,
		M:         m,
		encodeTab: encodeTab,
	}
}

func (e *erasurer) encodeKlauspost(data []byte) ([][]byte, error) {
	enc, err := reedsolomon.New(e.K, e.M)
	if err != nil {
		return nil, err
	}

	chunkSize := e.getChunkSize(len(data))

	encoded := make([][]byte, e.K+e.M)
	for i := 0; i < e.K+e.M; i++ {
		encoded[i] = make([]byte, chunkSize)
	}
	for i := 0; i < e.K; i++ {
		copy(encoded[i], data[i*chunkSize:(i+1)*chunkSize])
	}
	err = enc.Encode(encoded)
	return encoded, err
}
func (e *erasurer) encodeIsal(data []byte) ([][]byte, error) {
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

	C.ec_encode_data(C.int(chunkSize), C.int(e.K), C.int(e.M),
		(*C.uchar)(unsafe.Pointer(&e.encodeTab[0])),
		(**C.uchar)(unsafe.Pointer(&encodedBlocksPtr[:e.K][0])), // Pointers to data blocks
		(**C.uchar)(unsafe.Pointer(&encodedBlocksPtr[e.K:][0]))) // Pointers to parity blocks

	return encodedBlocks, nil
}

func (e *erasurer) getChunkSize(dataLen int) int {
	size := dataLen / e.K
	if dataLen%e.K > 0 {
		size += 1
	}
	return size
}
