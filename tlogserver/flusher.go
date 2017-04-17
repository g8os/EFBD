package main

// #include <isa-l_crypto/aes_cbc.h>
// #include <isa-l_crypto/aes_keyexp.h>
// #cgo LDFLAGS: -lisal_crypto
import "C"

import (
	"bytes"
	"fmt"
	"log"
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
	tlogs      map[uint32]*tlogTab
}

func newFlusher(conf *config) *flusher {
	// create redis pool
	pools := make(map[int]*redis.Pool, 1+conf.K+conf.M)
	for i := 0; i < conf.K+conf.M+1; i++ {
		addr := fmt.Sprintf("%v:%v", conf.firstObjStorAddr, conf.firstObjStorPort+i)
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
		tlogs:      map[uint32]*tlogTab{},
	}
}

func (f *flusher) getTlogTab(volID uint32) *tlogTab {
	tab, ok := f.tlogs[volID]
	if !ok {
		tab = newTlogTab(volID)
		f.tlogs[volID] = tab
	}
	return tab
}

/*
func (f *flusher) periodicFlush() {
	tick := time.NewTicker(1 * time.Second)
	for range tick {
		for volID, logs := range f.tlogs {
			f.tlogMutex.RLock()
			f.tlogMutex.RUnlock()
		}
	}
}*/

// store a tlog message and check if we can flush.
func (f *flusher) store(tlb *TlogBlock) *response {
	// add blocks to tlog table
	tab := f.getTlogTab(tlb.VolumeId())
	tab.Add(tlb)

	// check if we can do flush and do it
	seqs, err := f.checkDoFlush(tlb.VolumeId(), tab)
	if err != nil {
		return &response{
			Status: -1,
		}
	}
	status := int8(1)
	if err == nil && (seqs == nil || len(seqs) == 0) {
		seqs = []uint64{tlb.Sequence()}
		status = 0
	}

	return &response{
		Status:    status,
		Sequences: seqs,
	}
}

// check if we can flush and do it
func (f *flusher) checkDoFlush(volID uint32, tab *tlogTab) ([]uint64, error) {
	blocks, needFlush := tab.Pick(f.flushSize, f.flushTime, false)
	if !needFlush {
		return []uint64{}, nil
	}

	return f.flush(volID, blocks[:])
}

func (f *flusher) flush(volID uint32, blocks []*TlogBlock) ([]uint64, error) {
	log.Printf("flush @ vol id: %v, size:%v\n", volID, len(blocks))

	// capnp -> byte
	data, err := f.encodeCapnp(volID, blocks[:])
	if err != nil {
		return nil, err
	}

	// compress
	compressed := make([]byte, snappy.MaxEncodedLen(len(data)))
	compressed = snappy.Encode(compressed[:], data[:])

	encrypted := f.encrypt(compressed)

	// erasure
	erEncoded, err := f.erasure.encodeIsal(encrypted[:])
	if err != nil {
		return nil, err
	}

	// store to ardb
	if err := f.storeEncoded(volID, blake2b.Sum256(encrypted), erEncoded); err != nil {
		return nil, err
	}

	seqs := make([]uint64, len(blocks))
	for i := 0; i < len(blocks); i++ {
		seqs[i] = blocks[i].Sequence()
	}

	return seqs[:], nil
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

func (f *flusher) encodeCapnp(volID uint32, blocks []*TlogBlock) ([]byte, error) {
	// create capnp aggregation
	msg, seg, err := capnp.NewMessage(capnp.MultiSegment(nil))
	if err != nil {
		return nil, err
	}
	agg, err := NewRootTlogAggregation(seg)
	if err != nil {
		return nil, err
	}

	agg.SetName("The Go Tlog")
	agg.SetSize(uint64(len(blocks)))
	blockList, err := agg.NewBlocks(int32(len(blocks)))
	if err != nil {
		return nil, err
	}

	// add blocks
	for i := 0; i < blockList.Len(); i++ {
		blockList.Set(i, *blocks[i])
	}

	// create buffer
	dataSize := (f.flushSize * f.packetSize) + 200 // TODO:make it precise
	data := make([]byte, dataSize)
	buf := bytes.NewBuffer(data)
	buf.Truncate(0)

	err = capnp.NewEncoder(buf).Encode(msg)
	return buf.Bytes(), err
}