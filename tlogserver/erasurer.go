package main

// #include <isa-l/erasure_code.h>
// #include "erasurer.h"
// #cgo LDFLAGS: -lisal
import "C"
import (
	"unsafe"

	"github.com/templexxx/reedsolomon"
)

type erasurer struct {
	K          int
	M          int
	encodeTab  []byte                     // isa-l encode table
	rsEncoders map[string]*reedsolomon.Rs // templexxx/reedsolomon encoders
}

func newErasurer(k, m int) *erasurer {
	encodeTab := make([]byte, 32*k*(k+m))

	C.init_encode_tab(C.int(k), C.int(m), (*C.uchar)(unsafe.Pointer(&encodeTab[0])))
	return &erasurer{
		K:          k,
		M:          m,
		encodeTab:  encodeTab,
		rsEncoders: map[string]*reedsolomon.Rs{},
	}
}

// get reedsolomon encoder object
func (e *erasurer) getRsEncoder(volID string) (*reedsolomon.Rs, error) {
	rs, ok := e.rsEncoders[volID]
	if ok {
		return rs, nil
	}

	rs, err := reedsolomon.New(e.K, e.M)
	if err != nil {
		return nil, err
	}

	e.rsEncoders[volID] = rs
	return rs, nil
}

func (e *erasurer) encode(volID string, data []byte) ([][]byte, error) {
	/*enc, err := e.getRsEncoder(volID)
	if err != nil {
		return nil, err
	}
	return e.encodeTemplex(enc, data)
	*/

	return e.encodeIsal(data)
}

func (e *erasurer) encodeTemplex(enc *reedsolomon.Rs, data []byte) ([][]byte, error) {
	encoded := e.allocateEncodedBlocks(data[:])

	err := enc.Encode(encoded)
	return encoded, err
}

func (e *erasurer) encodeIsal(data []byte) ([][]byte, error) {
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

func (e *erasurer) allocateEncodedBlocks(data []byte) [][]byte {
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

func (e *erasurer) getChunkSize(dataLen int) int {
	size := dataLen / e.K
	if dataLen%e.K > 0 {
		size++
	}
	return size
}
