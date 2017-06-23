package decoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/snappy"
	"github.com/templexxx/reedsolomon"
	"github.com/zero-os/0-Disk/log"
	"zombiezen.com/go/capnproto2"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
)

// Decoder defines tlog data decoder
type Decoder struct {
	vdiskID string

	// erasure encoder variable
	k int
	m int

	// encryption variable
	decrypter tlog.AESDecrypter

	// pools of redis connection
	pool tlog.RedisPool
}

// DecodedAggregation defines a decoded tlog aggregation
// from decoder.
type DecodedAggregation struct {
	Agg *schema.TlogAggregation
	Err error
}

// New creates a tlog decoder
func New(pool tlog.RedisPool, k, m int, vdiskID, privKey, hexNonce string) (*Decoder, error) {
	if pool == nil {
		return nil, errors.New("Decoder requires a non-nil RedisPool")
	}
	if pool.DataConnectionCount() < k+m {
		return nil, errors.New("invalid number of objstor")
	}

	// create decrypter
	decrypter, err := tlog.NewAESDecrypter(privKey, hexNonce)
	if err != nil {
		return nil, err
	}

	return &Decoder{
		vdiskID:   vdiskID,
		k:         k,
		m:         m,
		decrypter: decrypter,
		pool:      pool,
	}, nil
}

func (d *Decoder) Close() {
	d.pool.Close()
}

// Decode decodes all tlog transaction started from
// startTs timestamp to endTs timestamp.
// If startTs == 0, it means from the beginning of transaction.
// If endTs == 0, it means until the end of transaction
func (d *Decoder) Decode(lmt Limiter) <-chan *DecodedAggregation {
	daChan := make(chan *DecodedAggregation, 1)

	go func() {
		defer close(daChan)

		// get all keys after the specified timestamp
		keys, err := d.getKeysAfter(lmt)
		if err != nil {
			da := &DecodedAggregation{
				Agg: nil,
				Err: err,
			}
			daChan <- da
			return
		}

		// get the aggregations
		for i := len(keys) - 1; i >= 0; i-- {
			key := keys[i]
			agg, err := d.get(key)
			da := &DecodedAggregation{
				Agg: agg,
				Err: err,
			}
			daChan <- da

			// return if this aggregation is errored
			if err != nil {
				return
			}
			blocks, err := agg.Blocks()
			if err != nil {
				return
			}

			// return if it is the last aggregation we want
			if lmt.EndAgg(agg, blocks) {
				return
			}
		}
	}()
	return daChan
}

// getAllKeys of this vdisk ID after specified timestamp
func (d *Decoder) getKeysAfter(lmt Limiter) ([][]byte, error) {

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
		blocks, err := agg.Blocks()
		if err != nil {
			return keys, err
		}

		// add this aggregation
		keys = append(keys, key)

		if lmt.StartAgg(agg, blocks) {
			return keys, nil
		}
		// check if we already in very first key
		prev, err := agg.Prev()
		if err != nil {
			return keys, err
		}

		// this is very first key
		if bytes.Compare(prev, tlog.FirstAggregateHash.Bytes()) == 0 {
			return keys, nil
		}

		key = prev
	}
}

// returns the max timestamps of all the contained blocks
func maxAggTimestamp(agg *schema.TlogAggregation) (uint64, error) {
	blocks, err := agg.Blocks()
	if err != nil {
		return 0, err
	}
	return blocks.At(blocks.Len() - 1).Timestamp(), nil
}

func minAggTimestamp(agg *schema.TlogAggregation) (uint64, error) {
	blocks, err := agg.Blocks()
	if err != nil {
		return 0, err
	}
	return blocks.At(0).Timestamp(), nil
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

	merged = merged[uint64Size : uint64Size+realLen]

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

// get and merge all pieces of the data
func (d *Decoder) getAllPieces(key []byte) ([]byte, error) {
	lost := []int{}
	length := d.k + d.m
	pieces := make([][]byte, length)

	var pieceSize int

	// get pieces from storage
	for i := 0; i < length; i++ {
		rc := d.pool.DataConnection(i)
		defer rc.Close()

		b, err := redis.Bytes(rc.Do("GET", key))
		if err != nil {
			log.Infof("tlog decoder : failed to get piece num:%v, key:%v,err:%v", i, key, err)
			lost = append(lost, i)
			continue
		}

		pieces[i] = b
		pieceSize = len(b)
	}

	if len(lost) > d.m {
		return nil, fmt.Errorf("tlog decoder : too much lost pieces:%v, max:%v", len(lost), d.m)
	}

	// erasure encoded it
	if len(lost) > 0 {
		// fill up the pieces size
		for _, v := range lost {
			pieces[v] = make([]byte, pieceSize)
		}

		rs, err := reedsolomon.New(d.k, d.m)
		if err != nil {
			return nil, err
		}
		if err := rs.Reconst(pieces, lost, true); err != nil {
			return nil, err
		}
	}

	// merge the pieces
	all := make([]byte, 0, len(pieces[0])*d.k)
	for i := 0; i < d.k; i++ {
		all = append(all, pieces[i]...)
	}
	return all, nil
}

// get last hash of a vdisk
func (d *Decoder) getLastHash() ([]byte, error) {
	return d.GetLastHash()
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

const (
	uint64Size = 8
)
