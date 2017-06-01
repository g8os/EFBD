package ardb

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"

	"github.com/g8os/blockstor/log"
	"github.com/g8os/blockstor/redisstub"
	"github.com/g8os/blockstor/tlog"
	"github.com/g8os/blockstor/tlog/tlogserver/server"
	"github.com/stretchr/testify/assert"
)

func TestTlogStorageWithInMemory(t *testing.T) {
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorage(t, vdiskID, blockSize, storage)
}

func TestTlogStorageForceFlushWithInMemory(t *testing.T) {
	const (
		vdiskID   = "a"
		blockSize = 8
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageForceFlush(t, vdiskID, blockSize, storage)
}

func TestTlogStorageWithInMemoryDeadlock(t *testing.T) {
	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	storage := newInMemoryStorage(vdiskID, blockSize)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageDeadlock(t, vdiskID, blockSize, blockCount, storage)
}

func TestTlogStorageWithDeduped(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	if !assert.NotNil(t, memRedis) {
		return
	}

	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 8
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestDedupedStorage(t, vdiskID, blockSize, blockCount, redisProvider)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorage(t, vdiskID, blockSize, storage)
}

func TestTlogStorageForceFlushWithDeduped(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	if !assert.NotNil(t, memRedis) {
		return
	}

	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 8
		blockCount = 8
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestDedupedStorage(t, vdiskID, blockSize, blockCount, redisProvider)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageForceFlush(t, vdiskID, blockSize, storage)
}

func TestTlogStorageDeadlockWithDeduped(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	if !assert.NotNil(t, memRedis) {
		return
	}

	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestDedupedStorage(t, vdiskID, blockSize, blockCount, redisProvider)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageDeadlock(t, vdiskID, blockSize, blockCount, storage)
}

func TestTlogStorageWithNondeduped(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	if !assert.NotNil(t, memRedis) {
		return
	}

	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID   = "a"
		blockSize = 8
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, blockSize, redisProvider)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorage(t, vdiskID, blockSize, storage)
}

func TestTlogStorageForceFlushWithNondeduped(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	if !assert.NotNil(t, memRedis) {
		return
	}

	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID   = "a"
		blockSize = 8
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, blockSize, redisProvider)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageForceFlush(t, vdiskID, blockSize, storage)
}

func TestTlogStorageDeadlockWithNondeduped(t *testing.T) {
	memRedis := redisstub.NewMemoryRedis()
	if !assert.NotNil(t, memRedis) {
		return
	}

	go memRedis.Listen()
	defer memRedis.Close()

	const (
		vdiskID    = "a"
		blockSize  = 128
		blockCount = 512
	)

	redisProvider := newTestRedisProvider(memRedis, nil) // root = nil
	storage := createTestNondedupedStorage(t, vdiskID, blockSize, redisProvider)
	if !assert.NotNil(t, storage) {
		return
	}

	testTlogStorageDeadlock(t, vdiskID, blockSize, blockCount, storage)
}

func testTlogStorage(t *testing.T, vdiskID string, blockSize int64, storage backendStorage) {
	tlogrpc := newTlogTestServer(t)
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	storage, err := newTlogStorage(vdiskID, tlogrpc, blockSize, storage)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}

	testBackendStorage(t, storage)
}

func testTlogStorageForceFlush(t *testing.T, vdiskID string, blockSize int64, storage backendStorage) {
	tlogrpc := newTlogTestServer(t)
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	storage, err := newTlogStorage(vdiskID, tlogrpc, blockSize, storage)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}

	testBackendStorageForceFlush(t, storage)
}

func testTlogStorageDeadlock(t *testing.T, vdiskID string, blockSize, blockCount int64, storage backendStorage) {
	tlogrpc := newTlogTestServer(t)
	if !assert.NotEmpty(t, tlogrpc) {
		return
	}

	storage, err := newTlogStorage(vdiskID, tlogrpc, blockSize, storage)
	if !assert.NoError(t, err) || !assert.NotNil(t, storage) {
		return
	}

	testBackendStorageDeadlock(t, blockSize, blockCount, storage)
}

func newTlogTestServer(t *testing.T) string {
	testConf := &server.Config{
		K:          4,
		M:          2,
		ListenAddr: "",
		FlushSize:  25,
		FlushTime:  25,
		PrivKey:    "12345678901234567890123456789012",
		HexNonce:   "37b8e8a308c354048d245f6d",
	}

	// create inmemory redis pool factory
	poolFactory := tlog.InMemoryRedisPoolFactory(testConf.RequiredDataServers())
	if !assert.NotNil(t, poolFactory) {
		return ""
	}

	// start the server
	s, err := server.NewServer(testConf, poolFactory)
	if !assert.NoError(t, err) {
		return ""
	}
	go s.Listen()

	return s.ListenAddr()
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
				rand.Read(preContent)
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
