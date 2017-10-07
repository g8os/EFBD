package tlog

import (
	"context"
	crand "crypto/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb/storage"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/stor"
	"github.com/zero-os/0-Disk/tlog/tlogserver/server"
)

// Test that include waiting for other tlog to be finished
// see : https://github.com/zero-os/0-Disk/issues/527
// it tests the idea described at  https://github.com/zero-os/0-Disk/issues/527#issuecomment-334711480
// Test scenario :
// - create 0-stor clusters
// - create nbdserver 1 (nb1) and tlogserver 1 (t1)
// 		- t1 listen at coordListenAddr
// - create nbd2 & t2 with same vdisk
//		- t2 need to wait for t1
//		- t2 use same 0-stor cluster as t1
// - nbd1 write X data
//		- then flush it
// - nbd2 write Y data
// 		- then flush it
// - stop nbd1 & t1
// - nbd2 write Z data
// 		- then flush it
// - stop nbd2 & t2
// - verify that tlog has X+Y+Z data with correct sequence
func TestWaitOtherTlog(t *testing.T) {
	const (
		vdiskID   = "aaa"
		blockSize = int64(4096)
	)

	tlogConf := server.DefaultConfig()
	tlogConf.ListenAddr = ""

	// parent context
	parentCtx, parentCancelFunc := context.WithCancel(context.Background())
	defer parentCancelFunc()

	// creates 0-stor for both tlog servers
	storSource, _, storCleanup := newZeroStorConfig(t, vdiskID, tlogConf)
	defer storCleanup()

	// creates nbd1 & tlog1
	ctx1, cancelFunc1 := context.WithCancel(parentCtx)
	defer cancelFunc1()

	tlogConf.CoordListenAddr = "-" // "-" means random port : TODO : make it constant
	t1, err := server.NewServer(tlogConf, storSource)
	require.NoError(t, err)
	go t1.Listen(ctx1)

	nbd1, nbd1Clean := newTestTlogStorage(ctx1, t, vdiskID, t1.ListenAddr(), blockSize)
	defer nbd1Clean()

	// creates nbd2 & tlog2
	ctx2, cancelFunc2 := context.WithCancel(parentCtx)
	defer cancelFunc2()

	tlogConf.CoordConnectAddr = t1.CoordListenAddr()
	t2, err := server.NewServer(tlogConf, storSource)
	require.NoError(t, err)
	go t2.Listen(ctx2)

	nbd2, nbd2Clean := newTestTlogStorage(ctx2, t, vdiskID, t2.ListenAddr(), blockSize)
	defer nbd2Clean()

	var (
		datas = make(map[uint64][]byte)
		seq   = tlog.FirstSequence
		wg2   sync.WaitGroup
	)

	// write some data to nbd1
	for i := 1; i <= tlogConf.FlushSize; i++ {
		data := make([]byte, blockSize)
		crand.Read(data)
		datas[seq] = data

		err = nbd1.SetBlock(int64(seq), data)
		require.NoError(t, err)

		seq++
	}

	// flush nbd1
	err = nbd1.Flush()
	require.NoError(t, err)

	// write some data to nbd2
	for i := 1; i <= tlogConf.FlushSize; i++ {
		data := make([]byte, blockSize)
		crand.Read(data)
		datas[seq] = data

		err = nbd2.SetBlock(int64(seq), data)
		require.NoError(t, err)

		seq++
	}

	// flush nbd2 in goroutine
	// because nbd2.Flush can only be finished after nbd1 terminated
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		err = nbd2.Flush()
		require.NoError(t, err)
	}()

	// stop nbd1 & tlog1
	nbd1.Close()
	cancelFunc1()

	// wait until nbd2 flush finished
	wg2.Wait()

	// write some data again to nbd2
	for i := 1; i <= tlogConf.FlushSize; i++ {
		data := make([]byte, blockSize)
		crand.Read(data)
		datas[seq] = data

		err = nbd2.SetBlock(int64(seq), data)
		require.NoError(t, err)

		seq++
	}

	// flush nbd2
	err = nbd2.Flush()
	require.NoError(t, err)
	nbd2.Close()
	cancelFunc2()

	// verify that tlog has the right data
	storCli, err := stor.NewClientFromConfigSource(storSource, vdiskID, tlogConf.PrivKey, tlogConf.DataShards,
		tlogConf.ParityShards)
	require.NoError(t, err)

	for wr := range storCli.Walk(0, tlog.TimeNowTimestamp()) {
		require.NoError(t, wr.Err)
		agg := wr.Agg
		blocks, err := agg.Blocks()
		require.NoError(t, err)
		for i := 0; i < int(agg.Size()); i++ {
			data, err := blocks.At(i).Data()
			require.NoError(t, err)

			seq := blocks.At(i).Sequence()

			require.Equal(t, datas[seq], data)
			delete(datas, seq)
		}
	}
	require.Equal(t, 0, len(datas))
}

func newTestTlogStorage(ctx context.Context, t *testing.T, vdiskID, tlogServerAddr string,
	blockSize int64) (storage.BlockStorage, func()) {

	blockStorage := storage.NewInMemoryStorage(vdiskID, blockSize)
	require.NotNil(t, blockStorage)

	source := config.NewStubSource()
	source.SetTlogServerCluster(vdiskID, "tlogcluster", &config.TlogClusterConfig{
		Servers: []string{tlogServerAddr},
	})

	tlogStorage, err := Storage(ctx, vdiskID, "tlogcluster", source, blockSize,
		blockStorage, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, tlogStorage)

	return tlogStorage, func() {
		source.Close()
	}
}
