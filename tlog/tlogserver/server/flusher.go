package server

import (
	"bytes"
	"encoding/binary"
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

const (
	lastHashPrefix = "last_hash_"
	respChanSize   = 10

	// tlogblock buffer size = flusher.flushSize * tlogBlockFactorSize
	// With buffer size that bigger than flushSize:
	// - we don't always block when flushing
	// - our RAM won't exploded because we still have upper limit
	tlogBlockFactorSize = 5
)

// channel of input tlog block and flusher response.
type tlogChan struct {
	inputChan chan *schema.TlogBlock
	respChan  chan *response
}

type flusher struct {
	k         int
	m         int
	flushSize int
	flushTime int
	privKey   []byte

	redisPool *tlog.RedisPool
	tlogChans map[string]tlogChan

	// platform-dependent interfaces
	erasure   erasure.EraruseCoder
	encrypter tlog.AESEncrypter
}

func newFlusher(conf *Config) (*flusher, error) {
	addresses, err := conf.ObjStoreServerAddresses()
	if err != nil {
		return nil, err
	}

	redisPool, err := tlog.NewRedisPool(addresses)
	if err != nil {
		return nil, fmt.Errorf("couldn't create flusher: %s", err.Error())
	}

	encrypter, err := tlog.NewAESEncrypter(conf.PrivKey, conf.HexNonce)
	if err != nil {
		return nil, err
	}

	f := &flusher{
		k:         conf.K,
		m:         conf.M,
		flushSize: conf.FlushSize,
		flushTime: conf.FlushTime,
		privKey:   []byte(conf.PrivKey),
		redisPool: redisPool,
		tlogChans: map[string]tlogChan{},
		erasure:   erasure.NewErasurer(conf.K, conf.M),
		encrypter: encrypter,
	}

	return f, nil
}

// get tlog channel for specific vdiskID
// or create it if needed.
func (f *flusher) getTlogChan(vdiskID string) (*tlogChan, error) {
	tlc, ok := f.tlogChans[vdiskID]
	if ok {
		return &tlc, nil
	}

	// get last hash from storage
	lastHash, err := f.getLastHash(vdiskID)
	if err != nil {
		return &tlc, err
	}

	// create the channel
	inputChan := make(chan *schema.TlogBlock, f.flushSize*tlogBlockFactorSize)
	respChan := make(chan *response, respChanSize)
	tlc = tlogChan{
		inputChan: inputChan,
		respChan:  respChan,
	}
	f.tlogChans[vdiskID] = tlc

	go f.vdiskFlusher(vdiskID, lastHash, &tlc)
	return &tlc, nil
}

// this is the flusher routine that does the flush asynchronously
func (f *flusher) vdiskFlusher(vdiskID string, lastHash []byte, tlc *tlogChan) {
	var err error

	tlogs := []*schema.TlogBlock{}
	dur := time.Duration(f.flushTime) * time.Second
	pfTimer := time.NewTimer(dur) // periodic flush timer

	var toFlushLen int
	for {
		select {
		case tlb := <-tlc.inputChan:
			tlogs = append(tlogs, tlb)
			if len(tlogs)%f.flushSize != 0 { // only flush if it reach f.flushSize
				continue
			}
			toFlushLen = f.flushSize

			pfTimer.Stop()
			pfTimer.Reset(dur)

		case <-pfTimer.C:
			pfTimer.Reset(dur)
			if len(tlogs) == 0 {
				continue
			}
			toFlushLen = len(tlogs)
		}

		// get the blocks
		blocks := tlogs[:toFlushLen]
		tlogs = tlogs[toFlushLen:]

		var seqs []uint64
		var status int8

		lastHash, seqs, err = f.flush(vdiskID, blocks[:], lastHash)
		if err != nil {
			log.Infof("flush %v failed: %v", vdiskID, err)
			status = -1
		}

		tlc.respChan <- &response{
			Status:    status,
			Sequences: seqs,
		}
	}
}

func (f *flusher) flush(vdiskID string, blocks []*schema.TlogBlock, lastHash []byte) ([]byte, []uint64, error) {
	// capnp -> byte
	data, err := f.encodeCapnp(vdiskID, blocks[:], lastHash)
	if err != nil {
		return lastHash, nil, err
	}

	// compress
	compressed := make([]byte, snappy.MaxEncodedLen(len(data)))
	compressed = snappy.Encode(compressed[:], data[:])

	// encrypt
	encrypted := f.encrypter.Encrypt(compressed)

	// add info about the original length of the message.
	// we do it because erasure encoder will append some data
	// to the message to make it aligned.
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, uint64(len(encrypted))); err != nil {
		return lastHash, nil, err
	}

	finalData := append(buf.Bytes(), encrypted...)

	// erasure
	erEncoded, err := f.erasure.Encode(vdiskID, finalData[:])
	if err != nil {
		return lastHash, nil, err
	}

	hash := blake2b.Sum256(encrypted)
	lastHash = hash[:]
	// store to ardb
	if err := f.storeEncoded(vdiskID, lastHash, erEncoded); err != nil {
		return lastHash, nil, err
	}

	seqs := make([]uint64, len(blocks))
	for i := 0; i < len(blocks); i++ {
		seqs[i] = blocks[i].Sequence()
	}

	return lastHash, seqs[:], nil
}

func (f *flusher) storeEncoded(vdiskID string, key []byte, encoded [][]byte) error {
	var wg sync.WaitGroup

	length := f.k + f.m
	wg.Add(length + 1)

	var errGlob error
	// store encoded data
	for i := 0; i < length; i++ {
		go func(idx int) {
			defer wg.Done()

			blocks := encoded[idx]
			rc := f.redisPool.Get(idx + 1)
			defer rc.Close()

			_, err := rc.Do("SET", key, blocks)
			if err != nil {
				log.Infof("error during flush idx %v: %v", idx, err)
				errGlob = err
			}
		}(i)
	}

	// store last hash name
	go func() {
		defer wg.Done()
		err := f.storeLastHash(vdiskID, key)
		if err != nil {
			log.Infof("error when setting last hash name: %v", err)
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
	agg.SetVdiskID(vdiskID)

	blockList, err := agg.NewBlocks(int32(len(blocks)))
	if err != nil {
		return nil, err
	}

	// add blocks
	for i := 0; i < blockList.Len(); i++ {
		block := blockList.At(i)
		if err := schema.CopyBlock(&block, blocks[i]); err != nil {
			return nil, err
		}
	}

	buf := new(bytes.Buffer)

	err = capnp.NewEncoder(buf).Encode(msg)

	return buf.Bytes(), err
}

func (f *flusher) getLastHash(vdiskID string) ([]byte, error) {
	rc := f.redisPool.Get(0)
	defer rc.Close()

	hash, err := redis.Bytes(rc.Do("GET", f.lastHashKey(vdiskID)))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	// if this is first aggregation, use priv key as last hash value
	// TODO : check with @robvanmieghem.
	if err == redis.ErrNil {
		hash = f.privKey
	}
	return hash, nil

}

func (f *flusher) storeLastHash(vdiskID string, lastHash []byte) error {
	rc := f.redisPool.Get(0)
	defer rc.Close()

	_, err := rc.Do("SET", f.lastHashKey(vdiskID), lastHash)
	return err
}

func (f *flusher) lastHashKey(vdiskID string) string {
	return lastHashPrefix + vdiskID
}
