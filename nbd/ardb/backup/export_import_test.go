package backup

import (
	"context"
	"crypto/rand"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/redisstub"
	"github.com/zero-os/0-Disk/testdata"
)

func TestImportExportCommute_8_2_16_MS(t *testing.T) {
	testImportExportCommute(t, 8, 2, 16, newInMemoryStorage)
}

func TestImportExportCommute_16_8_32_MS(t *testing.T) {
	testImportExportCommute(t, 16, 8, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_8_32_MS(t *testing.T) {
	testImportExportCommute(t, 64, 8, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_8_32_DS(t *testing.T) {
	testImportExportCommute(t, 64, 8, 32, newDedupedStorage)
}

func TestImportExportCommute_8_8_32_MS(t *testing.T) {
	testImportExportCommute(t, 8, 8, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_64_32_MS(t *testing.T) {
	testImportExportCommute(t, 64, 64, 32, newInMemoryStorage)
}

func TestImportExportCommute_64_64_32_DS(t *testing.T) {
	testImportExportCommute(t, 64, 64, 32, newDedupedStorage)
}

func TestImportExportCommute_8_16_32_MS(t *testing.T) {
	testImportExportCommute(t, 8, 16, 32, newInMemoryStorage)
}

func TestImportExportCommute_8_64_32_MS(t *testing.T) {
	testImportExportCommute(t, 8, 64, 32, newInMemoryStorage)
}

func TestImportExportCommute_8_64_32_DS(t *testing.T) {
	testImportExportCommute(t, 8, 64, 32, newDedupedStorage)
}

func TestImportExportCommute_4096_131072_256_MS(t *testing.T) {
	testImportExportCommute(t, 4096, 131072, 256, newInMemoryStorage)
}

func TestImportExportCommute_4096_131072_128_DS(t *testing.T) {
	testImportExportCommute(t, 4096, 131072, 128, newDedupedStorage)
}

type storageGenerator func(t *testing.T, vdiskID string, blockSize int64) (storage.BlockStorage, func())

func newInMemoryStorage(t *testing.T, vdiskID string, blockSize int64) (storage.BlockStorage, func()) {
	storage := storage.NewInMemoryStorage(vdiskID, blockSize)
	return storage, func() {
		storage.Close()
	}
}

func newDedupedStorage(t *testing.T, vdiskID string, blockSize int64) (storage.BlockStorage, func()) {
	redisProvider := redisstub.NewInMemoryRedisProvider(nil)
	storage, err := storage.Deduped(vdiskID, blockSize, ardb.DefaultLBACacheLimit, false, redisProvider)
	if err != nil {
		t.Fatal(err)
	}
	return storage, func() {
		redisProvider.Close()
		storage.Close()
	}
}

func testImportExportCommute(t *testing.T, srcBS, dstBS, blockCount int64, sgen storageGenerator) {
	assert := assert.New(t)

	ibm, indices := generateImportExportData(srcBS, blockCount)

	const (
		vdiskID = "foo"
	)

	var err error

	// ctx used for this test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup source in-memory storage
	srcMS, srcMSClose := sgen(t, vdiskID, srcBS)
	defer srcMSClose()
	// store all blocks in the source
	for index, block := range ibm {
		err = srcMS.SetBlock(index, block)
		if !assert.NoError(err) {
			return
		}
	}
	err = srcMS.Flush()
	if !assert.NoError(err) {
		return
	}

	// setup stub driver to use for this test
	driver := newStubDriver()

	// export source in-memory storage
	exportCfg := exportConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    srcBS,
		DstBlockSize:    dstBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = exportBS(ctx, srcMS, indices, driver, exportCfg)
	if !assert.NoError(err) {
		return
	}

	// setup destination in-memory storage
	dstMS, dstMSClose := sgen(t, vdiskID, srcBS)
	defer dstMSClose()

	// import into destination in-memory storage
	importCfg := importConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    dstBS,
		DstBlockSize:    srcBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = importBS(ctx, driver, dstMS, importCfg)
	if !assert.NoError(err) {
		return
	}

	err = dstMS.Flush()
	if !assert.NoError(err) {
		return
	}

	var srcBlock, dstBlock []byte

	// ensure that both source and destination contain
	// the same blocks for the same indices
	for _, index := range indices {
		srcBlock, err = srcMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		dstBlock, err = dstMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		assert.Equal(srcBlock, dstBlock)
	}
}

func generateImportExportData(blockSize, blockCount int64) (map[int64][]byte, []int64) {
	indexBlockMap := make(map[int64][]byte, blockCount)
	indices := make([]int64, blockCount)

	for i := int64(0); i < blockCount; i++ {
		data := make([]byte, blockSize)
		rand.Read(data)

		indexBlockMap[i] = data
		indices[i] = i
	}

	return indexBlockMap, indices
}

func TestImportExportCommute_src2_dst8_c8_o0_i1_MS(t *testing.T) {
	testImportExportCommuteWithOffsetAndInterval(t, 2, 8, 8, 0, 1, newInMemoryStorage)
}

func TestImportExportCommute_src2_dst8_c8_o4_i9_MS(t *testing.T) {
	testImportExportCommuteWithOffsetAndInterval(t, 2, 8, 8, 4, 9, newInMemoryStorage)
}

func TestImportExportCommute_src2_dst8_c8_o4_i9_DS(t *testing.T) {
	testImportExportCommuteWithOffsetAndInterval(t, 2, 8, 8, 4, 9, newDedupedStorage)
}

func TestImportExportCommute_src8_dst2_c8_o4_i9_MS(t *testing.T) {
	testImportExportCommuteWithOffsetAndInterval(t, 8, 2, 8, 4, 9, newInMemoryStorage)
}

func TestImportExportCommute_src8_dst8_c8_o5_i27_MS(t *testing.T) {
	testImportExportCommuteWithOffsetAndInterval(t, 8, 8, 8, 5, 27, newInMemoryStorage)
}

func TestImportExportCommute_src4096_dst131072_c8_o5_i27_DS(t *testing.T) {
	testImportExportCommuteWithOffsetAndInterval(t, 4096, 131072, 64, 3, 5, newDedupedStorage)
}

func testImportExportCommuteWithOffsetAndInterval(t *testing.T, srcBS, dstBS, blockCount, offset, interval int64, sgen storageGenerator) {
	assert := assert.New(t)

	ibm, indices := generateImportExportDataWithOffsetAndInterval(srcBS, blockCount, offset, interval)

	const (
		vdiskID = "foo"
	)

	var err error

	// ctx used for this test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup source in-memory storage
	srcMS, srcMSClose := sgen(t, vdiskID, srcBS)
	defer srcMSClose()
	// store all blocks in the source
	for index, block := range ibm {
		err = srcMS.SetBlock(index, block)
		if !assert.NoError(err) {
			return
		}
	}
	err = srcMS.Flush()
	if !assert.NoError(err) {
		return
	}

	// setup stub driver to use for this test
	driver := newStubDriver()

	// export source in-memory storage
	exportCfg := exportConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    srcBS,
		DstBlockSize:    dstBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = exportBS(ctx, srcMS, indices, driver, exportCfg)
	if !assert.NoError(err) {
		return
	}

	// setup destination in-memory storage
	dstMS, dstMSClose := sgen(t, vdiskID, srcBS)
	defer dstMSClose()

	// import into destination in-memory storage
	importCfg := importConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    dstBS,
		DstBlockSize:    srcBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = importBS(ctx, driver, dstMS, importCfg)
	if !assert.NoError(err) {
		return
	}

	err = dstMS.Flush()
	if !assert.NoError(err) {
		return
	}

	var srcBlock, dstBlock []byte

	// ensure that both source and destination contain
	// the same blocks for the same indices
	for _, index := range indices {
		srcBlock, err = srcMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		dstBlock, err = dstMS.GetBlock(index)
		if !assert.NoError(err) {
			continue
		}

		assert.Equal(srcBlock, dstBlock)
	}
}

func generateImportExportDataWithOffsetAndInterval(blockSize, blockCount, offset, interval int64) (map[int64][]byte, []int64) {
	indexBlockMap := make(map[int64][]byte, blockCount)
	indices := make([]int64, blockCount)

	for i := int64(0); i < blockCount; i++ {
		index := offset + i*interval
		data := generateSequentialDataBlock(index*blockSize, blockSize)
		indexBlockMap[index] = data
		indices[i] = index
	}

	return indexBlockMap, indices
}

func TestImportExportCommute_StaticCases_MS(t *testing.T) {
	testImportExportCommuteStaticCases(t, newInMemoryStorage)
}

func TestImportExportCommute_StaticCases_DS(t *testing.T) {
	testImportExportCommuteStaticCases(t, newDedupedStorage)
}

func testImportExportCommuteStaticCases(t *testing.T, sgen storageGenerator) {
	for index, testCase := range staticTestSourceDataSlices {
		t.Logf("testing case %d (2 <-> 8)", index)
		testImportExportCommuteStatic(t, testCase, 2, 8, sgen)
		t.Logf("testing case %d (8 <-> 2)", index)
		testImportExportCommuteStatic(t, testCase, 8, 2, sgen)
		t.Logf("testing case %d (2 <-> 2)", index)
		testImportExportCommuteStatic(t, testCase, 2, 2, sgen)
		t.Logf("testing case %d (8 <-> 8)", index)
		testImportExportCommuteStatic(t, testCase, 8, 8, sgen)
	}
}

func testImportExportCommuteStatic(t *testing.T, sourceData []byte, srcBS, dstBS int64, sgen storageGenerator) {
	assert := assert.New(t)

	blockCount := int64(len(sourceData)) / srcBS

	ibm := make(map[int64][]byte)
	var indices []int64

	// collect source data into our familiar mapping and listing
	for i := int64(0); i < blockCount; i++ {
		start := i * srcBS
		end := start + srcBS
		block := sourceData[start:end]
		if isNilBlock(block) {
			continue
		}

		ibm[i] = block
		indices = append(indices, i)
	}

	const (
		vdiskID = "foo"
	)

	var err error

	// ctx used for this test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup source in-memory storage
	srcMS, srcMSClose := sgen(t, vdiskID, srcBS)
	defer srcMSClose()
	// store all blocks in the source
	for index, block := range ibm {
		err = srcMS.SetBlock(index, block)
		if !assert.NoError(err) {
			return
		}
	}
	err = srcMS.Flush()
	if !assert.NoError(err) {
		return
	}

	// setup stub driver to use for this test
	driver := newStubDriver()

	// export source in-memory storage
	exportCfg := exportConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    srcBS,
		DstBlockSize:    dstBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = exportBS(ctx, srcMS, indices, driver, exportCfg)
	if !assert.NoError(err) {
		return
	}

	// setup destination in-memory storage
	dstMS, dstMSClose := sgen(t, vdiskID, srcBS)
	defer dstMSClose()

	// import into destination in-memory storage
	importCfg := importConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    dstBS,
		DstBlockSize:    srcBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = importBS(ctx, driver, dstMS, importCfg)
	if !assert.NoError(err) {
		return
	}

	err = dstMS.Flush()
	if !assert.NoError(err) {
		return
	}

	var srcBlock, dstBlock []byte

	// ensure that both source and destination contain
	// the same blocks for the same indices
	for _, index := range indices {
		srcBlock, err = srcMS.GetBlock(index)
		if !assert.NoErrorf(err, "index: %v", index) {
			continue
		}

		dstBlock, err = dstMS.GetBlock(index)
		if !assert.NoErrorf(err, "index: %v", index) {
			continue
		}

		assert.Equalf(srcBlock, dstBlock, "index: %v", index)
	}
}

func TestImportExportCommute_LedeImg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	t.Log("load lede image from gzipped testdata")
	ibm := getLedeImageBlocks()

	var indices []int64

	// collect all indices and sort them
	for index := range ibm {
		indices = append(indices, index)
	}
	sort.Sort(int64Slice(indices))

	bs := int64(testdata.LedeImageBlockSize)

	t.Log("test (in-memory) import-export commut using export blocks of 4096 bytes (same size as source)")
	testImportExportCommuteUsingPremadeData(t, ibm, indices, bs, bs, newInMemoryStorage)

	t.Log("test (in-memory) import-export commut using the export blocks of 131072 bytes (production default)")
	testImportExportCommuteUsingPremadeData(t, ibm, indices, bs, 131072, newInMemoryStorage)

	t.Log("test (deduped w/ ledisdb) import-export commut using the export blocks of 131072 bytes (production default)")
	testImportExportCommuteUsingPremadeData(t, ibm, indices, bs, 131072, newDedupedStorage)
}

func testImportExportCommuteUsingPremadeData(t *testing.T, ibm map[int64][]byte, indices []int64, srcBS, dstBS int64, sgen storageGenerator) {
	assert := assert.New(t)

	const (
		vdiskID = "foo"
	)

	var err error

	// ctx used for this test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup source in-memory storage
	srcMS, srcMSClose := sgen(t, vdiskID, srcBS)
	defer srcMSClose()
	// store all blocks in the source
	for index, block := range ibm {
		err = srcMS.SetBlock(index, block)
		if !assert.NoError(err) {
			return
		}
	}
	err = srcMS.Flush()
	if !assert.NoError(err) {
		return
	}

	// setup stub driver to use for this test
	driver := newStubDriver()

	// export source in-memory storage
	exportCfg := exportConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    srcBS,
		DstBlockSize:    dstBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = exportBS(ctx, srcMS, indices, driver, exportCfg)
	if !assert.NoError(err) {
		return
	}

	// setup destination in-memory storage
	dstMS, dstMSClose := sgen(t, vdiskID, srcBS)
	defer dstMSClose()

	// import into destination in-memory storage
	importCfg := importConfig{
		JobCount:        runtime.NumCPU(),
		SrcBlockSize:    dstBS,
		DstBlockSize:    srcBS,
		CompressionType: LZ4Compression,
		CryptoKey:       privKey,
		SnapshotID:      vdiskID,
	}
	err = importBS(ctx, driver, dstMS, importCfg)
	if !assert.NoError(err) {
		return
	}

	err = dstMS.Flush()
	if !assert.NoError(err) {
		return
	}

	var srcBlock, dstBlock []byte

	// ensure that both source and destination contain
	// the same blocks for the same indices
	for _, index := range indices {
		srcBlock, err = srcMS.GetBlock(index)
		if !assert.NoErrorf(err, "index: %v", index) {
			continue
		}

		dstBlock, err = dstMS.GetBlock(index)
		if !assert.NoErrorf(err, "index: %v", index) {
			continue
		}

		assert.Equalf(srcBlock, dstBlock, "index: %v", index)
	}
}

// int64Slice implements the sort.Interface for a slice of int64s
type int64Slice []int64

func (s int64Slice) Len() int           { return len(s) }
func (s int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func init() {
	log.SetLevel(log.DebugLevel)
}
