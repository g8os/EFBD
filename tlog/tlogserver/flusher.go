package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogserver/erasure"
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
	"github.com/golang/snappy"
	"github.com/minio/blake2b-simd"
	"zombiezen.com/go/capnproto2"
)

type flusher struct {
	k         int
	m         int
	flushSize int
	flushTime int
	privKey   []byte

	redisPools map[int]*redis.Pool // pools of redis connection
	tlogs      map[string]*tlogTab

	// platform-dependent interfaces
	erasure   erasure.EraruseCoder
	encrypter tlog.AESEncrypter
}

func newFlusher(conf *config) (*flusher, error) {
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

	encrypter, err := tlog.NewAESEncrypter(conf.privKey, conf.nonce)
	if err != nil {
		return nil, err
	}

	f := &flusher{
		k:          conf.K,
		m:          conf.M,
		flushSize:  conf.flushSize,
		flushTime:  conf.flushTime,
		privKey:    []byte(conf.privKey),
		redisPools: pools,
		tlogs:      map[string]*tlogTab{},
		erasure:    erasure.NewErasurer(conf.K, conf.M),
		encrypter:  encrypter,
	}

	go f.periodicFlush()
	return f, nil
}

func (f *flusher) getTlogTab(vdiskID string) *tlogTab {
	tab, ok := f.tlogs[vdiskID]
	if !ok {
		tab = newTlogTab(vdiskID)
		f.tlogs[vdiskID] = tab
	}
	return tab
}

func (f *flusher) periodicFlush() {
	tick := time.Tick(2 * time.Second)
	for range tick {
		for vdiskID, tab := range f.tlogs {
			f.checkDoFlush(vdiskID, tab, true)
		}
	}
}

// store a tlog message and check if we can flush.
func (f *flusher) store(tlb *schema.TlogBlock, vdiskID string) *response {
	// add blocks to tlog table
	tab := f.getTlogTab(vdiskID)
	tab.Add(tlb)

	// check if we can do flush and do it
	seqs, err := f.checkDoFlush(vdiskID, tab, false)
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
func (f *flusher) checkDoFlush(vdiskID string, tab *tlogTab, periodic bool) ([]uint64, error) {
	blocks, needFlush := tab.Pick(f.flushSize, f.flushTime, periodic)
	if !needFlush {
		return []uint64{}, nil
	}

	seqs, err := f.flush(vdiskID, blocks[:], tab)
	if err != nil {
		log.Infof("flush failed vor vdisk ID:%v, err:%v", vdiskID, err)
	}
	return seqs, err
}

func (f *flusher) flush(vdiskID string, blocks []*schema.TlogBlock, tab *tlogTab) ([]uint64, error) {
	log.Debugf("flush @ vdiskID: %v, size:%v\n", vdiskID, len(blocks))

	// get last hash
	lastHash, err := tab.getLastHash(f.redisPools[0].Get())
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	// if this is first aggregation, use priv key as last hash value
	// TODO : check with @robvanmieghem.
	if err == redis.ErrNil {
		lastHash = f.privKey
	}

	// capnp -> byte
	data, err := f.encodeCapnp(vdiskID, blocks[:], lastHash)
	if err != nil {
		return nil, err
	}

	// compress
	compressed := make([]byte, snappy.MaxEncodedLen(len(data)))
	compressed = snappy.Encode(compressed[:], data[:])

	// align it
	// We need to align it because erasure encode need the pieces to be
	// in same length.
	// We need to do it before encryption because otherwise the decrypter
	// will get different message than the original.
	alignLen := f.k - (len(compressed) % f.k)
	if alignLen > 0 {
		pad := make([]byte, alignLen)
		compressed = append(compressed, pad...)
	}

	// encrypt
	encrypted := f.encrypter.Encrypt(compressed)

	// erasure
	erEncoded, err := f.erasure.Encode(vdiskID, encrypted[:])
	if err != nil {
		return nil, err
	}

	hash := blake2b.Sum256(encrypted)
	// store to ardb
	if err := f.storeEncoded(vdiskID, hash[:], erEncoded, tab); err != nil {
		return nil, err
	}

	seqs := make([]uint64, len(blocks))
	for i := 0; i < len(blocks); i++ {
		seqs[i] = blocks[i].Sequence()
	}

	return seqs[:], nil
}

func (f *flusher) storeEncoded(vdiskID string, key []byte, encoded [][]byte, tab *tlogTab) error {
	var wg sync.WaitGroup

	wg.Add(f.k + f.m + 1)

	var errGlob error
	// store encoded data
	for i := 0; i < f.k+f.m; i++ {
		go func(idx int) {
			defer wg.Done()

			blocks := encoded[idx]
			rc := f.redisPools[idx+1].Get()
			_, err := rc.Do("SET", key, blocks)
			if err != nil {
				log.Debugf("error during flush idx %v:%v", idx, err)
				errGlob = err
			}
		}(i)
	}

	// store last hash name
	go func() {
		defer wg.Done()
		rc := f.redisPools[0].Get()
		err := tab.storeLastHash(rc, key)
		if err != nil {
			log.Debugf("error when setting last hash name:%v", err)
			errGlob = err
		}
	}()

	wg.Wait()
	return errGlob
}

func (f *flusher) encodeCapnp(vdiskID string, blocks []*schema.TlogBlock, lastHash []byte) ([]byte, error) {
	// create capnp aggregation
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	agg, err := schema.NewRootTlogAggregation(seg)
	if err != nil {
		return nil, err
	}

	agg.SetName("The Go Tlog")
	agg.SetSize(uint64(len(blocks)))
	agg.SetTimestamp(uint64(time.Now().UnixNano()))
	agg.SetPrev(lastHash)

	blockList, err := agg.NewBlocks(int32(len(blocks)))
	if err != nil {
		return nil, err
	}

	// add blocks
	for i := 0; i < blockList.Len(); i++ {
		blockList.Set(i, *blocks[i])
	}

	buf := new(bytes.Buffer)

	err = capnp.NewEncoder(buf).Encode(msg)

	return buf.Bytes(), err
}