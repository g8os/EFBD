package decoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/snappy"
	"zombiezen.com/go/capnproto2"

	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/schema"
)

var (
	ErrDecodeFinished = errors.New("decode finished.")
	ErrDecodeLost     = errors.New("decode lost in history.")
)

// Decoder defines tlog data decoder
type Decoder struct {
	objstorAddr []string
	vdiskID     string

	// erasure encoder variable
	k int
	m int

	// encryption variable
	decrypter tlog.AESDecrypter
	privKey   []byte

	// prefix of the last hash key
	lastHashPrefix string

	// pools of redis connection
	redisPools []*redis.Pool
}

// New creates a tlog decoder
func New(objstorAddr []string, k, m int, vdiskID, privKey, nonce string) (*Decoder, error) {
	if len(objstorAddr) != k+m+1 {
		return nil, fmt.Errorf("invalid number of objstor")
	}

	// create decrypter
	decrypter, err := tlog.NewAESDecrypter(privKey, nonce)
	if err != nil {
		return nil, err
	}

	// create redis pools
	pools := make([]*redis.Pool, len(objstorAddr))

	for i := 0; i < len(objstorAddr); i++ {
		addr := objstorAddr[i]
		pools[i] = &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
		}
	}

	return &Decoder{
		objstorAddr:    objstorAddr,
		vdiskID:        vdiskID,
		k:              k,
		m:              m,
		decrypter:      decrypter,
		privKey:        []byte(privKey),
		lastHashPrefix: "last_hash_",
		redisPools:     pools,
	}, nil
}

// Decode decodes the tlog with timestamp >= startTs.
// This func ends when the ErrDecodeFinished received from error channel.
func (d *Decoder) Decode(startTs uint64) (<-chan *schema.TlogAggregation, <-chan error) {
	aggChan := make(chan *schema.TlogAggregation, 1)
	errChan := make(chan error, 1)

	go func() {
		// get all keys after the specified timestamp
		keys, err := d.getKeysAfter(startTs)
		if err != nil {
			errChan <- err
			return
		}
		fmt.Printf("len keys=%v\n", len(keys))

		// get the aggregations
		for i := len(keys) - 1; i >= 0; i-- {
			key := keys[i]
			agg, err := d.get(key)
			if err != nil {
				errChan <- err
				return
			}
			aggChan <- agg
		}
		errChan <- ErrDecodeFinished
	}()
	return aggChan, errChan
}

// getAllKeys of this vdisk ID after specified timestamp
func (d *Decoder) getKeysAfter(startTs uint64) ([][]byte, error) {
	key, err := d.getLastHash()
	if err != nil {
		return nil, err
	}

	keys := [][]byte{}

	for {
		agg, err := d.get(key)
		if err != nil {
			return keys, err
		}

		// we'have passed the startTs
		if agg.Timestamp() < startTs {
			return keys, nil
		}

		keys = append(keys, key)

		// check if we already in very first key
		prev, err := agg.Prev()
		if err != nil {
			return keys, err
		}

		// this is very first key
		if bytes.Compare(prev, d.privKey) == 0 {
			return keys, nil
		}

		key = prev
	}

	return keys, ErrDecodeLost
}

// get tlog aggregation by it's key
func (d *Decoder) get(key []byte) (*schema.TlogAggregation, error) {
	merged, err := d.getAllPieces(key)
	if err != nil {
		return nil, err
	}

	// get the real len
	var realLen uint64
	if err := binary.Read(bytes.NewBuffer(merged), binary.LittleEndian, &realLen); err != nil {
		return nil, err
	}
	merged = merged[8 : 8+realLen] /* 8 is the size of uint64 */

	// decrypt
	decrypted, err := d.decrypt(merged)
	if err != nil {
		return nil, err
	}

	//uncompressed, err := d.uncompress(decrypted)
	uncompressed, err := snappy.Decode(nil, decrypted)
	if err != nil {
		return nil, err
	}

	return d.decodeCapnp(uncompressed)
}

func (d *Decoder) decrypt(encrypted []byte) ([]byte, error) {
	return d.decrypter.Decrypt(encrypted)
}

func (d *Decoder) getAllPieces(key []byte) ([]byte, error) {
	all := []byte{}
	for i := 0; i < d.k; i++ {
		rc := d.redisPools[i+1].Get()

		b, err := redis.Bytes(rc.Do("GET", key))
		if err != nil {
			return nil, fmt.Errorf("failed to get key:%v, err:%v", key, err)
		}

		all = append(all, b...)
	}
	return all, nil
}

func (d *Decoder) getLastHash() ([]byte, error) {
	rc := d.redisPools[0].Get()

	key := d.lastHashPrefix + d.vdiskID
	b, err := redis.Bytes(rc.Do("get", key))
	if err != nil {
		return nil, fmt.Errorf("failed to get key:%v, err:%v", key, err)
	}
	return b, nil
}

func (d *Decoder) decodeCapnp(data []byte) (*schema.TlogAggregation, error) {
	buf := bytes.NewBuffer(data)

	msg, err := capnp.NewDecoder(buf).Decode()
	if err != nil {
		return nil, err
	}

	agg, err := schema.ReadRootTlogAggregation(msg)
	return &agg, err
}
