package erasure

import (
	"github.com/templexxx/reedsolomon"
)

//EraruseCoder reedsolomon encodes
type EraruseCoder interface {
	//Encode actually encodes
	Encode(volID string, data []byte) ([][]byte, error)
}

//NewEraruser is the factory method for creating ErasureCoder instances
var NewEraruser = newErasurer

type erasurer struct {
	K          int
	M          int
	rsEncoders map[string]*reedsolomon.Rs // templexxx/reedsolomon encoders
}

func newErasurer(k, m int) EraruseCoder {
	return &erasurer{
		K:          k,
		M:          m,
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

func (e *erasurer) Encode(volID string, data []byte) ([][]byte, error) {
	enc, err := e.getRsEncoder(volID)
	if err != nil {
		return nil, err
	}
	return e.encodeTemplex(enc, data)
}

func (e *erasurer) encodeTemplex(enc *reedsolomon.Rs, data []byte) ([][]byte, error) {
	encoded := e.allocateEncodedBlocks(data[:])

	err := enc.Encode(encoded)
	return encoded, err
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
