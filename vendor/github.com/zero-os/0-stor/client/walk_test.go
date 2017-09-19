package client

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

// blockMap is dummy block.Writer/Reader used solely for tests
type blockMap struct {
	data    map[string][]byte
	metaCli *meta.Client
}

func newBlockMap(metaCli *meta.Client) *blockMap {
	return &blockMap{
		metaCli: metaCli,
		data:    make(map[string][]byte),
	}
}

func (bs *blockMap) WriteBlock(key, val []byte, md *meta.Meta) (*meta.Meta, error) {
	bs.data[string(key)] = val
	return md, bs.metaCli.Put(string(key), md)
}

func (bs *blockMap) ReadBlock(key []byte) ([]byte, error) {
	val := bs.data[string(key)]
	delete(bs.data, string(key))
	return val, nil
}

func TestWalk(t *testing.T) {
	testWalk(t, true)
}

func TestWalkBack(t *testing.T) {
	testWalk(t, false)
}

func testWalk(t *testing.T, forward bool) {
	etcd, err := embedserver.New()
	require.Nil(t, err)
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(t, 4)
	defer serverClean()

	dataShards := make([]string, len(servers))
	for i, server := range servers {
		dataShards[i] = server.Addr()
	}

	// client policy
	policy := Policy{
		Organization: "testorg",
		Namespace:    "thedisk",
		DataShards:   dataShards,
		MetaShards:   []string{etcd.ListenAddr()},
		IYOAppID:     "",
		IYOSecret:    "",
	}

	cli, err := getTestClient(policy)
	require.Nil(t, err)

	// override the storWriter and reader
	// bs := newBlockMap(cli.metaCli)
	// cli.storWriter = bs
	// cli.storReader = bs

	// create keys & data
	var keys [][]byte
	var vals [][]byte

	// initialize the data
	for i := 0; i < 100; i++ {
		key := make([]byte, 32)
		rand.Read(key)
		keys = append(keys, key)

		val := make([]byte, 1024)
		rand.Read(val)
		vals = append(vals, val)
	}

	startEpoch := time.Now().UTC().UnixNano()
	// do the write
	var prevMd *meta.Meta
	var prevKey []byte
	var firstKey []byte

	for i, key := range keys {
		prevMd, err = cli.WriteWithMeta(key, vals[i], prevKey, prevMd, nil, nil)
		require.NoError(t, err)
		prevKey = key
		if len(firstKey) == 0 {
			firstKey = key
		}
	}

	endEpoch := time.Now().UTC().UnixNano()

	// walk over it
	var wrCh <-chan *WalkResult
	if forward {
		wrCh = cli.Walk(firstKey, startEpoch, endEpoch)
	} else {
		wrCh = cli.WalkBack(firstKey, startEpoch, endEpoch)
	}

	var i int
	for {
		wr, ok := <-wrCh
		if !ok {
			break
		}
		require.Nil(t, wr.Error)
		idx := i
		if forward == false {
			idx = len(keys) - i
		}
		require.Equal(t, keys[idx], wr.Key)
		require.Equal(t, vals[idx], wr.Data)
		i++
	}
}
