package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
	"github.com/g8os/blockstor/tlog/tlogclient/decoder"
	"github.com/g8os/blockstor/tlog/tlogserver/erasure"
	"github.com/golang/snappy"
	"github.com/minio/blake2b-simd"
	"zombiezen.com/go/capnproto2"
)

const (
	lastHashNum = 5 // number of last hashes we want to save per vdisk ID
)

type flusher struct {
	k         int
	m         int
	flushSize int
	flushTime int
	privKey   string
	hexNonce  string

	pool tlog.RedisPool

	// platform-dependent interfaces
	erasure   erasure.EraruseCoder
	encrypter tlog.AESEncrypter
}

func newFlusher(conf *flusherConfig, pool tlog.RedisPool) (*flusher, error) {
	encrypter, err := tlog.NewAESEncrypter(conf.PrivKey, conf.HexNonce)
	if err != nil {
		return nil, err
	}

	f := &flusher{
		k:         conf.K,
		m:         conf.M,
		flushSize: conf.FlushSize,
		flushTime: conf.FlushTime,
		privKey:   conf.PrivKey,
		hexNonce:  conf.HexNonce,
		pool:      pool,
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
	if err := f.storeEncoded(vd, lastHash, erEncoded); err != nil {
		return nil, err
	}

	vd.lastHash = lastHash // only update last hash if everything is OK

	seqs := make([]uint64, len(blocks))
	for i := 0; i < len(blocks); i++ {
		seqs[i] = blocks[i].Sequence()
	}

	return seqs[:], nil
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

	return buf.Bytes(), err
}

func (f *flusher) storeEncoded(vd *vdisk, key []byte, encoded [][]byte) error {
	var wg sync.WaitGroup

	length := f.k + f.m
	wg.Add(length)

	var allErr []error

	// store encoded data
	for i := 0; i < length; i++ {
		go func(idx int) {
			defer wg.Done()

			blocks := encoded[idx]
			ef := f.storeAndRetry(idx, key, vd.lastHashKey, blocks)
			if !ef.Nil() {
				err := fmt.Errorf("error during flush idx %v: %v", idx, ef)
				allErr = append(allErr, err)
			}
		}(i)
	}

	wg.Wait()

	if len(allErr) > 0 {
		log.Infof("flush of vdiskID=%s failed: %v", vd.vdiskID, allErr)
		return fmt.Errorf("flush vdiskID=%s failed", vd.vdiskID)
	}

	return nil
}

type errStore struct {
	errSend     error
	errData     error
	errLastHash error
}

func (es errStore) Nil() bool {
	return es.errSend == nil && es.errData == nil && es.errLastHash == nil
}

func (es errStore) Error() string {
	if es.Nil() {
		return ""
	}

	var errs []string

	if es.errSend != nil {
		errs = append(errs, fmt.Sprintf("redis Send failed: %v", es.errSend))
	}
	if es.errData != nil {
		errs = append(errs, fmt.Sprintf("failed to send data pieces: %v", es.errData))
	}
	if es.errLastHash != nil {
		errs = append(errs, fmt.Sprintf("failed to update last hash: %v", es.errLastHash))
	}
	return strings.Join(errs, ",")
}

// store data to redis and retry it if failed
func (f *flusher) storeAndRetry(idx int, hash, lastHashKey, data []byte) errStore {
	var es errStore

	sleepMs := time.Duration(500) * time.Millisecond

	for i := 0; i < 4; i++ {
		es = f.store(idx, hash, lastHashKey, data)
		if es.Nil() {
			return es
		}
		log.Infof("error to store data in server idx:%v, err: %v", idx, es.Error())

		// sleep for a while
		time.Sleep(sleepMs * time.Duration(i))
	}
	return es
}

// store data to redis and retry it if failed
func (f *flusher) store(idx int, hash, lastHashKey, data []byte) errStore {
	var es errStore

	// get new connection inside this function.
	// so in case of error, we got new connection that hopefully better
	rc := f.pool.DataConnection(idx)
	defer rc.Close()

	// send command to store the data
	if err := rc.Send("SET", hash, data); err != nil {
		es.errSend = err
		return es
	}

	// send command to store the last hash
	if err := rc.Send("LPUSH", lastHashKey, hash); err != nil {
		es.errSend = err
		return es
	}

	// flush the commands
	if err := rc.Flush(); err != nil {
		es.errSend = err
		return es
	}

	// reply of set data
	if _, err := rc.Receive(); err != nil {
		es.errData = err
	}

	// reply of lpush last hash
	if _, err := rc.Receive(); err != nil {
		es.errLastHash = err
	}

	if !es.Nil() {
		return es
	}

	// we don't need to wait for the trim operation because it won't harm us
	// if failed
	go func() {
		rc := f.pool.DataConnection(idx)
		defer rc.Close()
		if _, err := rc.Do("LTRIM", lastHashKey, 0, lastHashNum); err != nil {
			log.Errorf("failed to trim lasth hash of %s: %s", lastHashKey, err.Error())
		}
	}()

	return es
}

func (f *flusher) getLastHash(vdiskID string) ([]byte, error) {
	dec, err := decoder.New(f.pool, f.k, f.m, vdiskID, f.privKey, f.hexNonce)
	if err != nil {
		return nil, err
	}

	hash, err := dec.GetLastHash()
	if err != nil && err != decoder.ErrNilLastHash {
		return nil, err
	}

	if err == decoder.ErrNilLastHash {
		hash = tlog.FirstAggregateHash
	}
	return hash, nil

}
