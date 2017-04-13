package main

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
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
	// erasure
	_, err = f.erasure.encode(data)
	if err != nil {
		return err
	}
	return nil
}

func (f *flusher) encodeCapnp(volID uint32) ([]byte, error) {
	// create buffer
	dataSize := (f.flushSize * f.packetSize) + 200 // TODO:make it precise
	data := make([]byte, dataSize)
	buf := bytes.NewBuffer(data)

	// create aggregation
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	agg, err := NewRootTlogAggregation(seg)
	if err != nil {
		return nil, err
	}

	agg.SetName("The Go Tlog")
	agg.SetSize(uint64(f.flushSize))
	blockList, err := agg.NewBlocks(int32(agg.Size()))
	if err != nil {
		return nil, err
	}

	// add blocks
	for i := 0; i < blockList.Len(); i++ {
		block := blockList.At(i)
		block.SetSequence(uint64(i))
		block.SetVolumeId(volID)
	}

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
