package main

// #include <isa-l_crypto/aes_cbc.h>
// #include <isa-l_crypto/aes_keyexp.h>
// #cgo LDFLAGS: -lisal_crypto
import "C"

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"
	"unsafe"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/snappy"
	"github.com/minio/blake2b-simd"
	"zombiezen.com/go/capnproto2"
)

type flusher struct {
	k          int
	m          int
	flushSize  int
	flushTime  int
	privKey    string
	packetSize int

	redisPools map[int]*redis.Pool
	erasure    *erasurer
	// TODO
	// improve the mutex to be more fine grained
	// make it one mutex per volumeID
	tlogs     map[uint32][]*TlogBlock
	tlogMutex sync.RWMutex
}

func newFlusher(conf *config) *flusher {
	// create redis pool
	pools := make(map[int]*redis.Pool, 1+conf.K+conf.M)
	for i := 0; i < conf.K+conf.M+1; i++ {
		addr := fmt.Sprintf("%v:%v", conf.firstObjStorAddr, conf.firstObjStorPort+i)
		fmt.Printf("addr:%v\n", addr)
		pools[i] = &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
		}
	}

	return &flusher{
		k:          conf.K,
		m:          conf.M,
		flushSize:  conf.flushSize,
		flushTime:  conf.flushTime,
		privKey:    conf.privKey,
		packetSize: conf.bufSize,
		redisPools: pools,
		erasure:    newErasurer(conf.K, conf.M),
		tlogs:      map[uint32][]*TlogBlock{},
	}
}

func (f *flusher) store(tlb *TlogBlock) {
	f.tlogMutex.Lock()
	defer f.tlogMutex.Unlock()
	f.tlogs[tlb.VolumeId()] = append(f.tlogs[tlb.VolumeId()], tlb)
}

func (f *flusher) checkDoFlush(volID uint32) error {
	if !f.okToFlush(volID, false) {
		return nil
	}
	return f.flush(volID)
}

func (f *flusher) flush(volID uint32) error {
	log.Printf("flush : %v\n", volID)
	// capnp -> byte
	data, err := f.encodeCapnp(volID)
	if err != nil {
		return err
	}
	// compress
	compressed := make([]byte, snappy.MaxEncodedLen(len(data)))
	compressed = snappy.Encode(compressed[:], data[:])

	encrypted := f.encrypt(compressed)

	// erasure
	er_encoded, err := f.erasure.encodeIsal(encrypted[:])
	if err != nil {
		return err
	}

	return f.storeEncoded(volID, blake2b.Sum256(encrypted), er_encoded)
}

func (f *flusher) encrypt(data []byte) []byte {
	// TODO : this initialization should be moved to somewhere else
	iv := make([]byte, 16)
	for i := 0; i < len(iv); i++ {
		iv[i] = '0'
	}
	encKey := make([]byte, 256)
	decKey := make([]byte, 256)
	bPrivKey := []byte(f.privKey)

	C.aes_keyexp_256((*C.uint8_t)(unsafe.Pointer(&bPrivKey[0])),
		(*C.uint8_t)(unsafe.Pointer(&encKey[0])),
		(*C.uint8_t)(unsafe.Pointer(&decKey[0])))

	// alignment
	alignSize := 16 - (len(data) % 16)
	if alignSize > 0 {
		pad := make([]byte, alignSize)
		data = append(data, pad...)
	}

	encrypted := make([]byte, len(data))

	// call the devil!!
	C.aes_cbc_enc_256((unsafe.Pointer(&data[0])),
		(*C.uint8_t)(unsafe.Pointer(&iv[0])),
		(*C.uint8_t)(unsafe.Pointer(&encKey[0])),
		(unsafe.Pointer(&encrypted[0])),
		(C.uint64_t)(len(data)))

	return encrypted[:]
}

func (f *flusher) storeEncoded(volID uint32, key [32]byte, encoded [][]byte) error {
	// store encoded data
	for i := 0; i < f.k+f.m; i++ {
		blocks := encoded[i]
		rc := f.redisPools[i+1].Get()
		_, err := rc.Do("SET", key, blocks)
		if err != nil {
			return err
		}
	}

	// store last hash name
	lastHashKey := fmt.Sprintf("last_hash_%v", volID)
	rc := f.redisPools[0].Get()
	_, err := rc.Do("SET", lastHashKey, key)
	return err
}

func (f *flusher) encodeCapnp(volID uint32) ([]byte, error) {
	f.tlogMutex.Lock()
	defer f.tlogMutex.Unlock()

	// create aggregation
	msg, seg, err := capnp.NewMessage(capnp.MultiSegment(nil))
	if err != nil {
		return nil, err
	}
	agg, err := NewRootTlogAggregation(seg)
	if err != nil {
		return nil, err
	}

	agg.SetName("The Go Tlog")
	agg.SetSize(uint64(f.flushSize))
	blockList, err := agg.NewBlocks(int32(f.flushSize))
	if err != nil {
		return nil, err
	}

	// add blocks
	for i := 0; i < blockList.Len(); i++ {
		// pop first block
		block := f.tlogs[volID][0]
		f.tlogs[volID] = f.tlogs[volID][1:]

		blockList.Set(i, *block)
	}

	// create buffer
	dataSize := (f.flushSize * f.packetSize) + 200 // TODO:make it precise
	data := make([]byte, dataSize)
	buf := bytes.NewBuffer(data)
	buf.Truncate(0)

	err = capnp.NewEncoder(buf).Encode(msg)
	return buf.Bytes(), err
}

func (f *flusher) okToFlush(volID uint32, periodic bool) bool {
	f.tlogMutex.RLock()
	defer f.tlogMutex.RUnlock()

	if !periodic && len(f.tlogs[volID]) < f.flushSize {
		return false
	}

	return true
}
