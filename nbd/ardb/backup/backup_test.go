package backup

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/zero-os/0-Disk"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidation(t *testing.T) {
	assert := assert.New(t)

	for _, validConfig := range validConfigs {
		assert.NoErrorf(validConfig.validate(), "%v", validConfig)
	}

	for _, invalidConfig := range invalidConfigs {
		assert.Errorf(invalidConfig.validate(), "%v", invalidConfig)
	}
}

func TestInflationBlockFetcher_2_to_8(t *testing.T) {
	testInflationBlockFetcher(t, 2, 8)
}

func TestInflationBlockFetcher_4_to_8(t *testing.T) {
	testInflationBlockFetcher(t, 4, 8)
}

func TestInflationBlockFetcher_2_to_16(t *testing.T) {
	testInflationBlockFetcher(t, 2, 16)
}

func testInflationBlockFetcher(t *testing.T, srcBS, dstBS int64) {
	assert := assert.New(t)
	if !assert.True(dstBS > srcBS) {
		return
	}

	stub := new(stubBlockFetcher)
	fetcher := newInflationBlockFetcher(stub, srcBS, dstBS)

	// functions should work (see: not panic)
	// even though we reached EOF
	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	// util function to help us testing
	testSingleBlock := func(srcIndex, dstIndex int64, srcSize, dstSize, dstOffset int64) {
		srcData, dstData := generateInflationDataPair(srcSize, dstSize, dstOffset)
		stub.AddBlock(srcIndex, srcData)

		// getting data (block+index) should be fine as well
		pair, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			assert.Equal(dstIndex, pair.Index)
			assert.Equal(dstData, pair.Block)
		}
	}

	// test with just one block, on various locations
	testSingleBlock(0, 0, srcBS, dstBS, 0)     // at pos #0
	testSingleBlock(1, 0, srcBS, dstBS, srcBS) // at pos #1
	srcIndex := (dstBS / srcBS) - 1
	testSingleBlock(srcIndex, 0, srcBS, dstBS, srcIndex*srcBS) // at last pos

	// test with enough blocks to get a full source block
	dstData := make([]byte, dstBS)
	rand.Read(dstData)
	for i := int64(0); i < dstBS; i += srcBS {
		stub.AddBlock(i/srcBS, dstData[i:i+srcBS])
	}
	pair, err := fetcher.FetchBlock()
	if assert.NoError(err) {
		assert.Equal(int64(0), pair.Index)
		assert.Equal(dstData, pair.Block)
	}
}

func TestInflationBlockFetcherSlice_s2_d8_o1_i0(t *testing.T) {
	testInflationBlockFetcherSlice(t, 2, 8, 1, 0)
}

func TestInflationBlockFetcherSlice_s2_d8_o1_i3(t *testing.T) {
	testInflationBlockFetcherSlice(t, 2, 8, 1, 3)
}

func TestInflationBlockFetcherSlice_s2_d8_o4_i9(t *testing.T) {
	testInflationBlockFetcherSlice(t, 2, 8, 4, 9)
}

func TestInflationBlockFetcherSlice_s2_d8_o4_i19(t *testing.T) {
	testInflationBlockFetcherSlice(t, 2, 8, 4, 19)
}

func TestInflationBlockFetcherSlice_s2_d8_o3_i0(t *testing.T) {
	testInflationBlockFetcherSlice(t, 2, 8, 3, 0)
}

func TestInflationBlockFetcherSlice_s4096_d131072_o512_i1024(t *testing.T) {
	testInflationBlockFetcherSlice(t, 4096, 131072, 512, 1024)
}

func TestInflationBlockFetcherSlice_s4096_d131072_o3333_i6144(t *testing.T) {
	testInflationBlockFetcherSlice(t, 4096, 131072, 3333, 6144)
}

func testInflationBlockFetcherSlice(t *testing.T, srcBS, dstBS, offset, interval int64) {
	assert := assert.New(t)
	if !assert.True(dstBS > srcBS) {
		return
	}

	// create stub and fetcher to use (later)
	stub := new(stubBlockFetcher)
	fetcher := newInflationBlockFetcher(stub, srcBS, dstBS)

	const (
		// there are more source blocks
		// but these are the blocks we'll fill with data
		srcBlockCount = 32
	)

	// generate destData
	totalDestSize := offset + (srcBS+interval)*srcBlockCount - interval
	totalDestSize += dstBS - (totalDestSize % dstBS)
	destData := make([]byte, totalDestSize)
	destBlockCount := totalDestSize / dstBS

	// functions should work (see: not panic)
	// even though we reached EOF
	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	// generate all filled data blocks, and add them to the destination block
	for i := int64(0); i < srcBlockCount; i++ {
		data := generateSequentialDataBlock(i*srcBS, srcBS)
		di := i*(srcBS+interval) + offset
		copy(destData[di:], data)
	}

	// add all non-nil blocks to stub block fetcher
	for i := int64(0); i < totalDestSize; i += srcBS {
		block := destData[i : i+srcBS]
		if !isNilBlock(block) {
			si := i / srcBS
			stub.AddBlock(si, block)
		}
	}

	// fetch all destination blocks, now that data is ready
	for i := int64(0); i < destBlockCount; i++ {
		start := i * dstBS
		end := start + dstBS
		block := destData[start:end]

		if isNilBlock(block) {
			continue
		}

		pair, err := fetcher.FetchBlock()
		if !assert.NoError(err) {
			continue
		}

		if !assert.Equal(i, pair.Index) {
			continue
		}
		assert.Equalf(block, pair.Block, "index: %v", i)
	}
}

func generateSequentialDataBlock(pos, size int64) []byte {
	data := make([]byte, size)
	for i := int64(0); i < size; i++ {
		data[i] = byte((pos + i) % 255)
	}
	return data
}

func generateInflationDataPair(srcSize, dstSize, dstOffset int64) ([]byte, []byte) {
	input := make([]byte, srcSize)
	rand.Read(input)

	output := make([]byte, dstSize)
	copy(output[dstOffset:], input)

	return input, output
}

func TestInflationBlockFetcherWithStaticSourceData(t *testing.T) {
	for index, testCase := range staticTestSourceDataSlices {
		t.Logf("testing case %d (2 -> 8)", index)
		testInflationBlockFetcherStatic(t, testCase, 2, 8)
		t.Logf("testing case %d (4 -> 8)", index)
		testInflationBlockFetcherStatic(t, testCase, 4, 8)
	}
}

func testInflationBlockFetcherStatic(t *testing.T, sourceData []byte, srcBS, dstBS int64) {
	assert := assert.New(t)
	if !assert.True(srcBS < dstBS) {
		return
	}

	stub := new(stubBlockFetcher)
	fetcher := newInflationBlockFetcher(stub, srcBS, dstBS)

	// functions should work (see: not panic)
	// even though we reached EOF
	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	sourceDataLength := int64(len(sourceData))

	// add all source blocks (which aren't nil)
	for i := int64(0); i < sourceDataLength; i += srcBS {
		start := i
		end := start + srcBS
		block := sourceData[start:end]

		if isNilBlock(block) {
			continue
		}

		index := i / srcBS
		stub.AddBlock(index, block)
	}

	// now try to read all dst blocks
	for i := int64(0); i < sourceDataLength; i += dstBS {
		start := i
		end := start + dstBS
		block := sourceData[start:end]

		if isNilBlock(block) {
			continue
		}

		index := i / dstBS
		pair, err := fetcher.FetchBlock()
		if !assert.NoError(err) {
			continue
		}
		assert.Equal(index, pair.Index)
		assert.Equal(block, pair.Block)
	}

	// now stub should be EOF
	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)
}

func TestDeflationBlockFetcher_8_to_2_with_i1_and_o0(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 2, 1, 0)
}

func TestDeflationBlockFetcher_8_to_2_with_i2_and_o0(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 2, 2, 0)
}

func TestDeflationBlockFetcher_8_to_2_with_i1_and_o1(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 2, 1, 1)
}

func TestDeflationBlockFetcher_8_to_2_with_i2_and_o1(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 2, 2, 1)
}

func TestDeflationBlockFetcher_8_to_4_with_i1_and_o0(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 1, 0)
}

func TestDeflationBlockFetcher_8_to_4_with_i2_and_o1(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 2, 1)
}

func TestDeflationBlockFetcher_8_to_4_with_i1_and_o1(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 1, 1)
}

func TestDeflationBlockFetcher_8_to_4_with_i2_and_o9(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 2, 9)
}

func TestDeflationBlockFetcher_8_to_4_with_i4_and_o14(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 4, 14)
}

func TestDeflationBlockFetcher_8_to_4_with_i1_and_o4(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 1, 4)
}

func TestDeflationBlockFetcher_8_to_4_with_i1_and_o2(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 1, 2)
}

func TestDeflationBlockFetcher_8_to_4_with_i2_and_o0(t *testing.T) {
	testDeflationBlockFetcher(t, 8, 4, 2, 0)
}

func TestDeflationBlockFetcher_16_to_2_with_i1_and_o0(t *testing.T) {
	testDeflationBlockFetcher(t, 16, 2, 1, 0)
}

func TestDeflationBlockFetcher_4096_to_512_with_i0_and_o0(t *testing.T) {
	testDeflationBlockFetcher(t, 4096, 512, 0, 0)
}

func TestDeflationBlockFetcher_4096_to_512_with_i333_and_o512(t *testing.T) {
	testDeflationBlockFetcher(t, 4096, 512, 333, 512)
}

func testDeflationBlockFetcher(t *testing.T, srcBS, dstBS, interval, offset int64) {
	assert := assert.New(t)
	if !assert.True(srcBS > dstBS) {
		return
	}

	stub := new(stubBlockFetcher)
	fetcher := newDeflationBlockFetcher(stub, srcBS, dstBS)

	// functions should work (see: not panic)
	// even though we reached EOF
	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	// try to read one full src block as multiple dst blocks
	// (do this 2 times, just because we can)
	for i := 0; i < 2; i++ {
		srcData := make([]byte, srcBS)
		rand.Read(srcData)
		stub.AddBlock((int64(i)+offset)*interval, srcData)

		ratio := srcBS / dstBS
		for u := int64(0); u < ratio; u++ {
			// getting data (block+index) should be correct
			pair, err := fetcher.FetchBlock()
			assert.Equalf((int64(i)+offset)*interval*ratio+u, pair.Index,
				"i = %d, u = %d", i, u)
			if assert.NoError(err) {
				start := u * dstBS
				end := start + dstBS
				assert.Equalf(srcData[start:end], pair.Block,
					"i = %d, u = %d", i, u)
			}
		}
	}

	// now stub should be EOF
	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	// try to read one src block, which has some nil dst blocks embedded.
	// (do this again 2 times, just because)
	srcData := make([]byte, srcBS)
	rand.Read(srcData)
	ratio := srcBS / dstBS
	// make all odd blocks (which includes the last one) nil
	for u := int64(1); u < ratio; u += 2 {
		start := u * dstBS
		end := start + dstBS
		copy(srcData[start:end], make([]byte, dstBS))
	}

	// add half-full src block (two times)
	stub.AddBlock(0, srcData)
	stub.AddBlock(1, srcData)

	for u := int64(0); u < ratio*2; u += 2 {
		// getting data (index+block) should be fine
		pair, err := fetcher.FetchBlock()
		if assert.NoError(err) {
			assert.Equalf(u, pair.Index, "u = %d", u)

			start := (u % ratio) * dstBS
			end := start + dstBS
			assert.Equalf(srcData[start:end], pair.Block, "u = %d", u)
		}
	}

	// now stub should be EOF
	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)
}

func TestDeflationBlockFetcherWithStaticSourceData(t *testing.T) {
	for index, testCase := range staticTestSourceDataSlices {
		t.Logf("testing case %d (8 -> 2)", index)
		testDeflationBlockFetcherStatic(t, testCase, 8, 2)
		t.Logf("testing case %d (8 -> 4)", index)
		testDeflationBlockFetcherStatic(t, testCase, 8, 4)
	}
}

func testDeflationBlockFetcherStatic(t *testing.T, sourceData []byte, srcBS, dstBS int64) {
	assert := assert.New(t)
	if !assert.True(srcBS > dstBS) {
		return
	}

	stub := new(stubBlockFetcher)
	fetcher := newDeflationBlockFetcher(stub, srcBS, dstBS)

	// functions should work (see: not panic)
	// even though we reached EOF
	_, err := fetcher.FetchBlock()
	assert.Equal(io.EOF, err)

	sourceDataLength := int64(len(sourceData))

	// add all source blocks (which aren't nil)
	for i := int64(0); i < sourceDataLength; i += srcBS {
		start := i
		end := start + srcBS
		block := sourceData[start:end]

		if isNilBlock(block) {
			continue
		}

		index := i / srcBS
		stub.AddBlock(index, block)
	}

	// now try to read all dst blocks
	for i := int64(0); i < sourceDataLength; i += dstBS {
		start := i
		end := start + dstBS
		block := sourceData[start:end]

		if isNilBlock(block) {
			continue
		}

		index := i / dstBS
		pair, err := fetcher.FetchBlock()
		if !assert.NoError(err) {
			continue
		}
		assert.Equal(index, pair.Index)
		assert.Equal(block, pair.Block)
	}

	// now stub should be EOF
	_, err = fetcher.FetchBlock()
	assert.Equal(io.EOF, err)
}

func TestOnceBlockFetcher(t *testing.T) {
	assert := assert.New(t)

	var once onceBlockFetcher

	// by default once should return io.EOF,
	// as the nil value of once contains a nil `blockIndexPair`.
	_, err := once.FetchBlock()
	assert.Equal(io.EOF, err)

	// when setting a pair to the onceBlockFetcher,
	// it will return that pair once and only once.
	pair := blockIndexPair{
		Index: 42,
		Block: zerodisk.Hash(make([]byte, 8)),
	}
	once.pair = &pair

	fetchedPair, err := once.FetchBlock()
	if assert.NoError(err) && assert.NotNil(fetchedPair) {
		assert.Equal(pair.Index, fetchedPair.Index)
		assert.Equal(pair.Block, fetchedPair.Block)
	}

	// block is already fetched
	_, err = once.FetchBlock()
	assert.Equal(io.EOF, err)
}

type stubBlockFetcher struct {
	Indices []int64
	Blocks  [][]byte
}

func (sbf *stubBlockFetcher) AddBlock(index int64, block []byte) {
	sbf.Indices = append(sbf.Indices, index)
	sbf.Blocks = append(sbf.Blocks, block)
}

// FetchBlock implements blockFetcher.FetchBlock
func (sbf *stubBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	if len(sbf.Indices) < 0 || len(sbf.Blocks) == 0 {
		return nil, io.EOF
	}

	block, index := sbf.Blocks[0], sbf.Indices[0]
	sbf.Indices, sbf.Blocks = sbf.Indices[1:], sbf.Blocks[1:]

	return &blockIndexPair{
		Block: block,
		Index: index,
	}, nil
}

func TestDeflationInflationCommute_2_4(t *testing.T) {
	testDeflationInflationCommute(t, 2, 4)
}

func TestDeflationInflationCommute_2_8(t *testing.T) {
	testDeflationInflationCommute(t, 2, 8)
}

func TestDeflationInflationCommute_2_64(t *testing.T) {
	testDeflationInflationCommute(t, 2, 64)
}

func testDeflationInflationCommute(t *testing.T, smallSize, bigSize int64) {
	assert := assert.New(t)

	sourceData := make([]byte, bigSize)
	rand.Read(sourceData)

	src := &onceBlockFetcher{&blockIndexPair{Block: sourceData}}
	bf := newInflationBlockFetcher(
		newDeflationBlockFetcher(src, bigSize, smallSize),
		smallSize, bigSize)

	pair, err := bf.FetchBlock()
	if assert.NoError(err) {
		assert.Equal(int64(0), pair.Index)
		assert.Equal(sourceData, pair.Block)
	}

	pair, err = bf.FetchBlock()
	if assert.Error(err) {
		assert.Nil(pair)
	}
}

func TestInflationDeflationCommute_4_2(t *testing.T) {
	testInflationDeflationCommute(t, 4, 2)
}

func TestInflationDeflationCommute_8_2(t *testing.T) {
	testInflationDeflationCommute(t, 8, 2)
}

func TestInflationDeflationCommute_16_4(t *testing.T) {
	testInflationDeflationCommute(t, 16, 4)
}

func TestInflationDeflationCommute_128_8(t *testing.T) {
	testInflationDeflationCommute(t, 128, 8)
}

func testInflationDeflationCommute(t *testing.T, bigSize, smallSize int64) {
	assert := assert.New(t)

	ratio := bigSize / smallSize

	// add source data
	sourceData := make(map[int64][]byte)
	src := new(stubBlockFetcher)
	for i := int64(0); i < ratio; i++ {
		data := make([]byte, smallSize)
		rand.Read(data)
		sourceData[i] = data
		src.AddBlock(i, data)
	}
	bf := newDeflationBlockFetcher(
		newInflationBlockFetcher(src, smallSize, bigSize),
		bigSize, smallSize)

	// fetch all blocks
	for i := int64(0); i < ratio; i++ {
		pair, err := bf.FetchBlock()
		if !assert.NoError(err) {
			return
		}
		if assert.Equal(i, pair.Index) {
			assert.Equal(sourceData[i], pair.Block)
		}
	}

	// should now be EOF, as we fetched all blocks
	pair, err := bf.FetchBlock()
	if assert.Error(err) {
		assert.Nil(pair)
	}
}

// onceBlockFetcher is a fetcher which returns a pair just once,
// after which it will return io.EOF, until a new pair is given.
type onceBlockFetcher struct {
	pair *blockIndexPair
}

// FetchBlock implements blockFetcher.FetchBlock
func (obf *onceBlockFetcher) FetchBlock() (*blockIndexPair, error) {
	if obf.pair == nil {
		return nil, io.EOF
	}

	pair := obf.pair
	obf.pair = nil
	return pair, nil
}

// used for:
//  + inflation fetcher tests;
//  + deflation fetcher tests;
//  + export<->import commutative tests;
var staticTestSourceDataSlices = [][]byte{
	[]byte{
		0, 1, 2, 3, 0, 0, 4, 5,
		6, 7, 8, 9, 0, 0, 0, 0,
		0, 0, 1, 2, 3, 0, 0, 4,
		0, 0, 0, 0, 0, 0, 0, 0,
		5, 0, 6, 0, 7, 0, 8, 0,
		0, 9, 0, 1, 0, 2, 0, 3,
	},
	[]byte{
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 1, 0, 0, 2, 3, 0, 0,
		0, 0, 4, 5, 0, 0, 6, 7,
		0, 0, 0, 8, 0, 0, 0, 9,
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 2, 0, 0, 0,
		0, 3, 0, 4, 0, 5, 0, 6,
		7, 8, 0, 0, 9, 1, 0, 0,
	},
}
