package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogserver/erasure"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/snappy"
	"github.com/minio/blake2b-simd"
	"zombiezen.com/go/capnproto2"
)

const (
	lastHashPrefix = "last_hash_"
)

type flusher struct {
	k         int
	m         int
	flushSize int
	flushTime int
	privKey   []byte

	redisPool *tlog.RedisPool

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
		erasure:   erasure.NewErasurer(conf.K, conf.M),
		encrypter: encrypter,
	}

	return f, nil
}

func (f *flusher) flush(blocks []*schema.TlogBlock, vd *vdisk) ([]uint64, error) {
	// capnp -> byte
	data, err := f.encodeCapnp(blocks[:], vd)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	finalData := append(buf.Bytes(), encrypted...)

	// erasure
	erEncoded, err := f.erasure.Encode(vd.vdiskID, finalData[:])
	if err != nil {
		return nil, err
	}

	hash := blake2b.Sum256(encrypted)
	lastHash := hash[:]
	// store to ardb
	if err := f.storeEncoded(vd.vdiskID, lastHash, erEncoded); err != nil {
		return nil, err
	}
	vd.lastHash = lastHash

	seqs := make([]uint64, len(blocks))
	for i := 0; i < len(blocks); i++ {
		seqs[i] = blocks[i].Sequence()
	}

	return seqs[:], nil
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

func (f *flusher) encodeCapnp(blocks []*schema.TlogBlock, vd *vdisk) ([]byte, error) {

	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(vd.segmentBuf))
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
	agg.SetPrev(vd.lastHash)
	agg.SetVdiskID(vd.vdiskID)

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

	vd.resizeSegmentBuf(buf.Len())

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
