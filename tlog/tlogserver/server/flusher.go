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
	lastHashNum = 5 // number of last hashes we want to save per vdisk ID
)

type flusher struct {
	k         int
	m         int
	flushSize int
	flushTime int

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

func (f *flusher) storeEncoded(vd *vdisk, key []byte, encoded [][]byte) error {
	var wg sync.WaitGroup

	length := f.k + f.m
	wg.Add(length + 1)

	var allErr []error

	// store encoded data
	for i := 0; i < length; i++ {
		go func(idx int) {
			defer wg.Done()

			blocks := encoded[idx]
			err := f.storeData(idx, "SET", key, blocks)
			if err != nil {
				err = fmt.Errorf("error during flush idx %v: %v", idx, err)
				allErr = append(allErr, err)
			}
		}(i)
	}

	// store last hash name
	lastHashStored := false
	go func() {
		defer wg.Done()
		err := f.storeLastHash(vd.vdiskID, key)
		if err != nil {
			err = fmt.Errorf("error when setting last hash name: %v", err)
			allErr = append(allErr, err)
		} else {
			lastHashStored = true
		}
	}()

	wg.Wait()

	if lastHashStored && len(allErr) > 0 {
		// if last hash successfully stored but we had error in data pieces,
		// we need to roolback the last_hash
		//
		// FIXME : what to do if the rollback itself got error?
		// - need to use other data structure?
		// - only store last hash after we successfully store all pieces?
		if err := f.storeLastHash(vd.vdiskID, vd.lastHash); err != nil {
			log.Infof("WARNING: failed to rollback last hash of %s to %v, err: %s",
				vd.vdiskID, vd.lastHash, err.Error())
		}
	}
	if len(allErr) > 0 {
		log.Infof("flush of vdiskID=%s failed: %v", vd.vdiskID, allErr)
		return fmt.Errorf("flush vdiskID=%s failed", vd.vdiskID)
	}
	return nil
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
	rc := f.pool.MetadataConnection()
	defer rc.Close()

	hash, err := redis.Bytes(rc.Do("GET", f.lastHashKey(vdiskID)))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	if err == redis.ErrNil {
		hash = tlog.FirstAggregateHash
	}
	return hash, nil

}

// we store last hash as array of lastHashNum last hashes.
func (f *flusher) storeLastHash(vdiskID string, lastHash []byte) error {
	key := []byte(f.lastHashKey(vdiskID))

	// we don't need to wait for the trim operation because it won't harm us
	// if failed
	go func() {
		rc := f.pool.MetadataConnection()
		defer rc.Close()
		if _, err := rc.Do("LTRIM", key, 0, lastHashNum); err != nil {
			log.Errorf("failed to trim lasth hash of %s: %s", vdiskID, err.Error())
		}
	}()

	return f.storeMetadataAndRetry("LPUSH", key, lastHash)
}

func (f *flusher) lastHashKey(vdiskID string) string {
	return tlog.LastHashPrefix + vdiskID
}

// store data to redis and retry it if failed
func (f *flusher) storeDataAndRetry(idx int, cmd string, key, data []byte) error {
	var err error

	sleepMs := time.Duration(500) * time.Millisecond

	for i := 0; i < 4; i++ {
		err = f.storeData(idx, cmd, key, data)
		if err == nil {
			return nil
		}
		log.Infof("error to store data in server idx:%v, err: %v", idx, err)

		// sleep for a while
		time.Sleep(sleepMs)
		sleepMs *= 2 // double the sleep time for the next iteration
	}
	return err
}

// store data to redis and retry it if failed
func (f *flusher) storeData(idx int, cmd string, key, data []byte) error {
	// get new connection inside this function.
	// so in case of error, we got new connection that hopefully better
	rc := f.pool.DataConnection(idx)
	defer rc.Close()

	_, err := rc.Do(cmd, key, data)
	return err
}

// store metadata to redis and retry it if failed
func (f *flusher) storeMetadataAndRetry(cmd string, key, data []byte) error {
	var err error

	sleepMs := time.Duration(500) * time.Millisecond

	for i := 0; i < 4; i++ {
		err = f.storeMetadata(cmd, key, data)
		if err == nil {
			return nil
		}
		log.Infof("error to store metadata, err: %v", err)

		// sleep for a while
		time.Sleep(sleepMs)
		sleepMs *= 2 // double the sleep time for the next iteration
	}
	return err
}

// store metadata to redis and retry it if failed
func (f *flusher) storeMetadata(cmd string, key, data []byte) error {
	// get new connection inside this function.
	// so in case of error, we got new connection that hopefully better
	rc := f.pool.MetadataConnection()
	defer rc.Close()

	_, err := rc.Do(cmd, key, data)
	return err
}
