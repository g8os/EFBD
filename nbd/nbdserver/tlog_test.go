package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	mrand "math/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/stor/embeddedserver"
	"github.com/zero-os/0-Disk/tlog/tlogclient/decoder"
	"github.com/zero-os/0-Disk/tlog/tlogclient/player"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestTlogStorageWithInMemory(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdiskID   = "a"
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorage(ctx, t, vdiskID, blockSize, storage)
}

func TestTlogStorageForceFlushWithInMemory(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageForceFlush(ctx, t, vdiskID, blockSize, storage)
}

func TestTlogStorageWithDeduped(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 8
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := storage.Deduped(
		vdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	if !assert.NoError(t, err) {
		return
	}

	testTlogStorage(ctx, t, vdiskID, blockSize, storage)
}

func TestTlogStorageForceFlushWithDeduped(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 8
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := storage.Deduped(
		vdiskID, blockSize,
		ardb.DefaultLBACacheLimit, false, redisProvider)
	if !assert.NoError(t, err) {
		return
	}

	testTlogStorageForceFlush(ctx, t, vdiskID, blockSize, storage)
}

func TestTlogStorageWithNondeduped(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdiskID   = "a"
		blockSize = 8
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := storage.NonDeduped(
		vdiskID, "", blockSize, false, redisProvider)
	if !assert.NoError(t, err) {
		return
	}

	testTlogStorage(ctx, t, vdiskID, blockSize, storage)
}

func TestTlogStorageForceFlushWithNondeduped(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	const (
		vdiskID   = "a"
		blockSize = 8
	)

	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := storage.NonDeduped(
		vdiskID, "", blockSize, false, redisProvider)
	if !assert.NoError(t, err) {
		return
	}

	testTlogStorageForceFlush(ctx, t, vdiskID, blockSize, storage)
}

func testTlogStorage(ctx context.Context, t *testing.T, vdiskID string, blockSize int64, storage storage.BlockStorage) {
	cleanup, tlogrpc := newTlogTestServer(ctx, t, vdiskID)
	defer cleanup()
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	source := config.NewStubSource()
	source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
		Servers: []string{tlogrpc},
	})
	defer source.Close()

	storage, err := newTlogStorage(
		ctx, vdiskID, "tlogcluster", source, blockSize, storage, nil)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}

	testBlockStorage(t, storage)
}

func testTlogStorageForceFlush(ctx context.Context, t *testing.T, vdiskID string, blockSize int64, storage storage.BlockStorage) {
	cleanup, tlogrpc := newTlogTestServer(ctx, t, vdiskID)
	defer cleanup()
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	source := config.NewStubSource()
	source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
		Servers: []string{tlogrpc},
	})
	defer source.Close()

	storage, err := newTlogStorage(
		ctx, vdiskID, "tlogcluster", source, blockSize, storage, nil)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}

	testBlockStorageForceFlush(t, storage)
}

func newTlogTestServer(ctx context.Context, t *testing.T, vdiskID string) (func(), string) {
	testConf := &server.Config{
		K:          1,
		M:          1,
		ListenAddr: "",
		FlushSize:  25,
		FlushTime:  25,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}

	cleanup, configSource, _ := newZeroStorConfig(t, vdiskID, testConf)

	// start the server
	s, err := server.NewServer(testConf, configSource)
	require.Nil(t, err)

	go s.Listen(ctx)
	s.IgnoreSignalOnce(syscall.SIGTERM)

	return cleanup, s.ListenAddr()
}

func TestTlogDedupedStorageReplay(t *testing.T) {
	var connProvider ardb.ConnProvider
	defer func() {
		if connProvider != nil {
			connProvider.Close()
		}
	}()

	createDedupedStorage := func(vdiskID string, vdiskSize, blockSize int64) (storage.BlockStorage, error) {
		if connProvider != nil {
			connProvider.Close()
		}

		connProvider = redisstub.NewInMemoryRedisProvider(nil)
		return storage.Deduped(
			vdiskID, blockSize,
			ardb.DefaultLBACacheLimit, false, connProvider)
	}

	testTlogStorageReplay(t, createDedupedStorage)
}

func TestTlogNonDedupedStorageReplay(t *testing.T) {
	var connProvider ardb.ConnProvider
	defer func() {
		if connProvider != nil {
			connProvider.Close()
		}
	}()

	createNonDedupedStorage := func(vdiskID string, vdiskSize, blockSize int64) (storage.BlockStorage, error) {
		if connProvider != nil {
			connProvider.Close()
		}

		connProvider = redisstub.NewInMemoryRedisProvider(nil)
		return storage.NonDeduped(
			vdiskID, "", blockSize, false, connProvider)
	}

	testTlogStorageReplay(t, createNonDedupedStorage)
}

type storageCreator func(vdiskID string, vdiskSize, blockSize int64) (storage.BlockStorage, error)

func testTlogStorageReplay(t *testing.T, storageCreator storageCreator) {
	const (
		vdiskID       = "myvdisk"
		blockSize     = 4096
		size          = 1024 * 64
		firstSequence = 0
	)

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	t.Log("1. Start a tlogserver;")

	testConf := &server.Config{
		K:          1,
		M:          1,
		ListenAddr: "",
		FlushSize:  1,
		FlushTime:  1,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}

	cleanup, configSource, _ := newZeroStorConfig(t, vdiskID, testConf)
	defer cleanup()

	t.Log("start the server")
	s, err := server.NewServer(testConf, configSource)
	if !assert.NoError(t, err) {
		return
	}

	t.Log("make tlog server listen")
	go s.Listen(ctx)

	var (
		tlogrpc = s.ListenAddr()
	)

	t.Logf("listen addr=%v", tlogrpc)

	t.Log("2. Start a tlog BlockStorage, which hash tlogclient integration;")

	internalStorage, err := storageCreator(vdiskID, size, blockSize)
	if !assert.NoError(t, err) {
		return
	}

	source := config.NewStubSource()
	source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
		Servers: []string{tlogrpc},
	})
	defer source.Close()

	storage, err := newTlogStorage(
		ctx, vdiskID, "tlogcluster", source, blockSize, internalStorage, nil)
	if !assert.NoError(t, err) {
		return
	}

	t.Log("3. Generate random data (with some partial and full zero blocks)")
	t.Log("   and write it to the tlog storage;")

	data := make([]byte, size)
	_, err = crand.Read(data)
	if !assert.Nil(t, err) {
		return
	}
	blocks := size / blockSize

	zeroBlock := make([]byte, blockSize)

	startTs := uint64(time.Now().UnixNano())
	var lastBlockTs uint64 // timestamp before the last block

	for i := 0; i < blocks; i++ {
		if i == blocks-1 {
			// we need to flush here so we have accurate lastBlockTs
			// it is because the 0-stor based tlog use timestamp in
			// metadata server rather than in the aggregation
			err = storage.Flush()
			require.Nil(t, err)

			lastBlockTs = uint64(time.Now().UnixNano())
		}

		offset := i * blockSize

		op := mrand.Int() % 10

		if op > 5 && op < 8 { // zero block
			err = storage.DeleteBlock(int64(i))
			if !assert.Nil(t, err) {
				return
			}

			copy(data[offset:], zeroBlock)
			continue
		}

		if op > 8 {
			// partial zero block
			r := mrand.Int()
			size := r % (blockSize / 2)
			offset := offset + (r % (blockSize / 4))
			copy(data[offset:], zeroBlock[:size])
		}

		err = storage.SetBlock(int64(i), data[offset:offset+blockSize])
		if !assert.NoError(t, err) {
			return
		}
	}

	t.Log("flush data")
	err = storage.Flush()
	if !assert.NoError(t, err) {
		return
	}

	t.Log("4. Validate that all the data is retrievable and correct;")
	for i := 0; i < blocks; i++ {
		offset := i * blockSize
		content, err := storage.GetBlock(int64(i))
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, normalizeTestBlock(data[offset:offset+blockSize]), content) {
			return
		}
	}

	t.Log("5. Wipe all data on the arbd (AKA create a new storage and memory redis, hehe)")
	t.Log("   this time without tlog integration though!!!!")
	storage, err = storageCreator(vdiskID, size, blockSize)
	if !assert.NoError(t, err) {
		return
	}

	t.Log("6. Validate that the data is no longer retrievable via the backend;")
	for i := 0; i < blocks; i++ {
		content, err := storage.GetBlock(int64(i))
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Nil(t, content) {
			return
		}
	}

	t.Log("7. Replay the tlog aggregations;")

	t.Log("replay from tlog except the last block")
	player, err := player.NewPlayerWithStorage(ctx, configSource, nil, storage, vdiskID,
		testConf.PrivKey, testConf.K, testConf.M)
	if !assert.NoError(t, err) {
		return
	}

	_, err = player.Replay(decoder.NewLimitByTimestamp(startTs, lastBlockTs))
	require.Nil(t, err)

	t.Log("8. Validate that all replayed data is again retrievable and correct;")

	// validate all except the last
	for i := 0; i < blocks-1; i++ {
		offset := i * blockSize
		content, err := storage.GetBlock(int64(i))
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, normalizeTestBlock(data[offset:offset+blockSize]), content) {
			return
		}
	}

	t.Log("9. make sure the last block still not retrieavable")
	{
		content, err := storage.GetBlock(int64(blocks - 1))
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Nil(t, content) {
			return
		}
	}

	t.Log("10 replay last block")
	_, err = player.Replay(decoder.NewLimitByTimestamp(lastBlockTs, 0))
	require.Nil(t, err)

	t.Log("11. Validate that last block is again retrievable and correct;")
	{
		offset := (blocks - 1) * blockSize
		content, err := storage.GetBlock(int64(blocks - 1))
		if !assert.Nil(t, err) {
			return
		}
		if !assert.Equal(t, normalizeTestBlock(data[offset:offset+blockSize]), content) {
			return
		}
	}
}

func normalizeTestBlock(block []byte) []byte {
	for _, b := range block {
		if b != 0 {
			return block
		}
	}

	return nil
}

func TestInMemorySequenceCache(t *testing.T) {
	sq := newInMemorySequenceCache()
	if !assert.NotNil(t, sq) {
		return
	}
	testSequenceCache(t, sq, func(sn, vn int) {
		assert.Equal(t, sn, len(sq.sequences))
		assert.Equal(t, vn, len(sq.values))
	})
}

func TestInMemorySequenceCacheMassEviction(t *testing.T) {
	sq := newInMemorySequenceCache()
	if !assert.NotNil(t, sq) {
		return
	}

	testSequenceCacheMassEviction(t, sq, func(sn, vn int) {
		assert.Equal(t, sn, len(sq.sequences))
		assert.Equal(t, vn, len(sq.values))
	})
}

func TestAggressiveInMemorySequenceCacheEviction(t *testing.T) {
	sq := newInMemorySequenceCache()
	if !assert.NotNil(t, sq) {
		return
	}
	testAggressiveSequenceCacheEviction(t, sq, func(sn, vn int) {
		assert.Equal(t, sn, len(sq.sequences))
		assert.Equal(t, vn, len(sq.values))
	})
}

func testSequenceCache(t *testing.T, sq sequenceCache, lengthTest func(sn, vn int)) {
	// Evicting should be OK, even though we have no values in cache
	elements := sq.Evict()
	assert.Empty(t, elements)
	elements = sq.Evict(1, 2, 3)
	assert.Empty(t, elements)

	// getting a value should result in a false OK value
	value, ok := sq.Get(1)
	assert.False(t, ok)
	assert.Nil(t, value)

	const sequenceCount = 8

	// Let's add some values,
	// which should return true, as none of these values already exist
	for i := 0; i < sequenceCount; i++ {
		var data []byte
		if i > 0 && i%2 == 0 {
			data = []byte{byte(i)}
		}

		assert.NoError(t, sq.Add(uint64(i), int64(i), data))
	}

	// we should now have `sequenceCount` sequences and values
	lengthTest(sequenceCount, sequenceCount)

	// getting these values should be easy and possible
	for i := 0; i < sequenceCount; i++ {
		data, ok := sq.Get(int64(i))
		if !assert.True(t, ok) {
			continue
		}

		var expectedData []byte
		if i > 0 && i%2 == 0 {
			expectedData = []byte{byte(i)}
		}

		assert.Equal(t, expectedData, data)
	}

	// we can add a new version of a sequence
	err := sq.Add(sequenceCount, 0, []byte("Hello"))
	assert.NoError(t, err)
	// we can however not add a new version with an existing sequence
	// but this will not return an error
	err = sq.Add(0, 0, nil)
	assert.NoError(t, err)

	// also add a new version for the second blockIndex
	err = sq.Add(sequenceCount+1, 1, []byte("World"))
	assert.NoError(t, err)

	// sequence count will now be `sequenceCount+2)`,
	// while values coult should still be sequenceCount
	lengthTest(sequenceCount+2, sequenceCount)

	// getting the first value should now return our greeting
	for i := 0; i < 2; i++ {
		value, ok := sq.Get(0)
		if !assert.True(t, ok) {
			continue
		}
		assert.Equal(t, []byte("Hello"), value)

		value, ok = sq.Get(1)
		if !assert.True(t, ok) {
			continue
		}
		assert.Equal(t, []byte("World"), value)
	}

	// time to evict all original sequences and the first overwritten sequence
	var sequencIndicesToEvict []uint64

	for i := uint64(0); i <= sequenceCount; i++ {
		sequencIndicesToEvict = append(sequencIndicesToEvict, i)
	}

	elements = sq.Evict(sequencIndicesToEvict...)
	if !assert.Len(t, elements, sequenceCount) {
		return
	}

	// the values AND sequence count should now both be 1
	lengthTest(1, 1)

	// data should be the original one (except the first one, which was overwritten)
	for blockIndex, data := range elements {
		var expectedData []byte
		if blockIndex == 0 {
			expectedData = []byte("Hello")
		} else if blockIndex%2 == 0 {
			expectedData = []byte{byte(blockIndex)}
		}
		assert.Equal(t, expectedData, data)
	}

	// getting all blocks (except second one), should now fail
	for i := 0; i < sequenceCount; i++ {
		if i == 1 {
			continue
		}

		value, ok := sq.Get(int64(i))
		if !assert.False(t, ok) {
			continue
		}
		assert.Nil(t, value)
	}

	// let's evict that one now as well...
	elements = sq.Evict(sequenceCount + 1)
	assert.Len(t, elements, 1)

	// the values AND sequence count should now both be 0
	lengthTest(0, 0)

	// and the only element should be our index 1, with value World
	value, ok = elements[1]
	if assert.True(t, ok) {
		assert.Equal(t, []byte("World"), value)
	}
}

func testSequenceCacheMassEviction(t *testing.T, sq sequenceCache, lengthTest func(sn, vn int)) {
	const (
		blockSize              = 128
		blockCount       int64 = 512
		repeatCount      int64 = 2
		innerRepeatCount int64 = 4
	)

	var (
		sequence uint64
	)

	lengthTest(0, 0)

	for repetiton := int64(0); repetiton < repeatCount; repetiton++ {
		var allSequences [][]uint64
		var allData [][][]byte

		for innerRepitition := int64(0); innerRepitition < innerRepeatCount; innerRepitition++ {
			var wg sync.WaitGroup

			var sequenceArr []uint64
			var dataArr [][]byte

			for blockIndex := int64(0); blockIndex < blockCount; blockIndex++ {
				wg.Add(1)

				preContent := make([]byte, blockSize)
				crand.Read(preContent)
				dataArr = append(dataArr, preContent)

				sequenceArr = append(sequenceArr, sequence)
				sequence++
				sequence := sequenceArr[blockIndex]

				go func(blockIndex int64) {
					defer wg.Done()

					// add content
					err := sq.Add(sequence, blockIndex, preContent)
					if err != nil {
						t.Fatal(err)
					}

					// get content
					postContent, ok := sq.Get(blockIndex)
					if !ok {
						t.Fatal("couldn't receive content for block ",
							blockIndex, "repetition ", repetiton)
					}
					if bytes.Compare(preContent, postContent) != 0 {
						t.Fatal(repetiton, blockIndex, " unexpected content received")
					}
				}(blockIndex)
			}

			allSequences = append(allSequences, sequenceArr)
			allData = append(allData, dataArr)

			wg.Wait()
		}

		sequenceCount := int(blockCount * innerRepeatCount)
		valueCount := int(blockCount)

		lengthTest(sequenceCount, valueCount)

		// evict all "odd-sequenced" versions first
		for index := int64(0); index < innerRepeatCount; index += 2 {
			allSequences := allSequences[index]
			allData := allData[index]

			elements := sq.Evict(allSequences...)

			sequenceCount -= int(blockCount)
			lengthTest(sequenceCount, valueCount)

			l := len(allSequences)
			if len(elements) != l {
				t.Fatal(repetiton, index, " unexpected length ", l, len(elements))
			}

			for blockIndex := int64(0); blockIndex < blockCount; blockIndex++ {
				data, ok := elements[blockIndex]
				if !ok {
					t.Fatal("couldn't find data for block ", blockIndex, repetiton, index)
					continue
				}

				assert.Equal(t, allData[blockIndex], data)
			}
		}

		// evict all even ones, only the last one should this time return elements
		for index := int64(1); index < innerRepeatCount; index += 2 {
			allSequences := allSequences[index]

			elements := sq.Evict(allSequences...)

			sequenceCount -= int(blockCount)

			if index < innerRepeatCount-1 {
				lengthTest(sequenceCount, valueCount)

				if len(elements) > 0 {
					t.Fatal(repetiton, index, " unexpected length ", len(elements), elements)
				}

				continue
			}

			sequenceCount = 0
			valueCount = 0
			lengthTest(sequenceCount, valueCount)

			allData := allData[index]

			l := len(allSequences)
			if len(elements) != l {
				t.Fatal(repetiton, index, " unexpected length ", l, len(elements))
			}

			for blockIndex := int64(0); blockIndex < blockCount; blockIndex++ {
				data, ok := elements[blockIndex]
				if !ok {
					t.Fatal("couldn't find data for block ", blockIndex, repetiton, index)
					continue
				}

				assert.Equal(t, allData[blockIndex], data)
			}
		}
	}

	lengthTest(0, 0)
}

func testAggressiveSequenceCacheEviction(t *testing.T, sq sequenceCache, lengthTest func(sn, vn int)) {
	const blockIndex = 0

	assert.NoError(t, sq.Add(1, blockIndex, nil))
	lengthTest(1, 1)

	assert.NoError(t, sq.Add(2, blockIndex, nil))
	lengthTest(2, 1)

	assert.NoError(t, sq.Add(3, blockIndex, nil))
	lengthTest(3, 1)

	// now we'll evict aggressivly
	elements := sq.Evict(3)
	if !assert.Len(t, elements, 1) {
		return
	}

	_, ok := elements[blockIndex]
	assert.True(t, ok)

	// both sequences and values should now be empty
	// even though we skipped quite a few sequences

	lengthTest(0, 0)
}

func TestDataHistory(t *testing.T) {
	dh := newDataHistory()
	if !assert.NotNil(t, dh) {
		return
	}

	// a newly created history is empty
	assert.True(t, dh.Empty())
	// getting latest should fail, as there is no history yet
	latest, ok := dh.Latest()
	if assert.False(t, ok) {
		assert.Nil(t, latest)
	}

	const sequenceCount = 8

	for i := uint64(0); i < sequenceCount; i++ {
		// trimming any unexisting index should fail
		_, ok := dh.Trim(i)
		assert.False(t, ok)

		// let's add it now
		data := []byte{byte(i)}
		assert.NoError(t, dh.Add(i, data))

		// when adding a value,
		// the index given has to be higher,
		// then the one previously given
		assert.Error(t, dh.Add(i, data))

		// latest should now be the one we just added
		value, ok := dh.Latest()
		if assert.True(t, ok) {
			assert.Equal(t, data, value)
		}

		// should not be empty longer
		assert.False(t, dh.Empty())
	}

	latestData := []byte{byte(sequenceCount - 1)}
	value, ok := dh.Latest()
	if assert.True(t, ok) {
		assert.Equal(t, latestData, value)
	}

	// let's now remove ~half of them
	index := sequenceCount / 2
	value, ok = dh.Trim(uint64(index))
	if assert.True(t, ok) {
		assert.Equal(t, []byte{byte(index)}, value)
	}

	// should still not be empty
	assert.False(t, dh.Empty())

	// latest should still be the same
	value, ok = dh.Latest()
	if assert.True(t, ok) {
		assert.Equal(t, latestData, value)
	}

	// we'll now remove all of them, except the last one
	// which should return the latest data
	value, ok = dh.Trim(sequenceCount - 2)
	if assert.True(t, ok) {
		assert.Equal(t, []byte{sequenceCount - 2}, value)
	}

	// the history is not yet empty
	assert.False(t, dh.Empty())

	// latest should still be the same
	value, ok = dh.Latest()
	if assert.True(t, ok) {
		assert.Equal(t, latestData, value)
	}

	// we'll now remove the last one
	value, ok = dh.Trim(sequenceCount - 1)
	if assert.True(t, ok) {
		assert.Equal(t, latestData, value)
	}

	// the history is now empty
	assert.True(t, dh.Empty())

	// and we can no longer get the latest, or trim
	latest, ok = dh.Latest()
	if assert.False(t, ok) {
		assert.Nil(t, latest)
	}
	for i := uint64(0); i < sequenceCount; i++ {
		_, ok := dh.Trim(i)
		assert.False(t, ok)
	}
}

func init() {
	log.SetLevel(log.DebugLevel)
}

func newZeroStorConfig(t *testing.T, vdiskID string, tlogConf *server.Config) (func(), *config.StubSource, stor.Config) {

	// stor server
	storCluster, err := embeddedserver.NewZeroStorCluster(tlogConf.K + tlogConf.M)
	require.Nil(t, err)

	var servers []config.ServerConfig
	for _, addr := range storCluster.Addrs() {
		servers = append(servers, config.ServerConfig{
			Address: addr,
		})
	}

	// meta server
	mdServer, err := embedserver.New()
	require.Nil(t, err)

	storConf := stor.Config{
		VdiskID:         vdiskID,
		Organization:    os.Getenv("iyo_organization"),
		Namespace:       "thedisk",
		IyoClientID:     os.Getenv("iyo_client_id"),
		IyoSecret:       os.Getenv("iyo_secret"),
		ZeroStorShards:  storCluster.Addrs(),
		MetaShards:      []string{mdServer.ListenAddr()},
		DataShardsNum:   tlogConf.K,
		ParityShardsNum: tlogConf.M,
		EncryptPrivKey:  tlogConf.PrivKey,
	}

	clusterID := "zero_stor_cluster_id"
	stubSource := config.NewStubSource()

	stubSource.SetTlogZeroStorCluster(vdiskID, clusterID, &config.ZeroStorClusterConfig{
		IYO: config.IYOCredentials{
			Org:       storConf.Organization,
			Namespace: storConf.Namespace,
			ClientID:  storConf.IyoClientID,
			Secret:    storConf.IyoSecret,
		},
		Servers: servers,
		MetadataServers: []config.ServerConfig{
			config.ServerConfig{
				Address: mdServer.ListenAddr(),
			},
		},
	})

	cleanFunc := func() {
		mdServer.Stop()
		storCluster.Close()
	}
	return cleanFunc, stubSource, storConf
}
