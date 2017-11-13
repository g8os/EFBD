package slavesync

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbd/ardb"
	cmd "github.com/zero-os/0-Disk/nbd/ardb/command"
	"github.com/zero-os/0-Disk/redisstub"
)

func TestSlaveClusterDoForAll(t *testing.T) {
	slice := redisstub.NewMemoryRedisSlice(4)
	defer slice.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := config.NewStubSource()
	defer cs.Close()

	const (
		vdiskID   = "foo"
		clusterID = "slave"
	)

	cfg := slice.StorageClusterConfig()
	cs.SetPrimaryStorageCluster(vdiskID, clusterID, nil)
	cs.SetSlaveStorageCluster(vdiskID, clusterID, &cfg)

	cluster, err := NewSlaveCluster(ctx, vdiskID, cs)
	if err != nil {
		t.Fatal(err)
	}

	testClusterDoForAll(t, cluster)
}

func testClusterDoForAll(t *testing.T, cluster ardb.StorageCluster) {
	require := require.New(t)

	const valueCount = 1024
	// base test - ensure these values don't exist yet using the DoFor metho
	for i := int64(0); i < valueCount; i++ {
		exists, err := ardb.Bool(cluster.DoFor(i, ardb.Command(cmd.Exists, fmt.Sprintf("foo:%d", i))))
		require.NoError(err)
		require.False(exists)
	}

	// now check the same with DoForAll
	var pairs []ardb.IndexActionPair
	for i := int64(0); i < valueCount; i++ {
		pairs = append(pairs, ardb.IndexActionPair{
			Index:  i,
			Action: ardb.Command(cmd.Exists, fmt.Sprintf("foo:%d", i)),
		})
	}
	replies, err := cluster.DoForAll(pairs)
	require.NoError(err)
	require.Len(replies, valueCount)
	for _, reply := range replies {
		exists, err := ardb.Bool(reply, nil)
		require.NoError(err)
		require.False(exists)
	}

	// now let's store the index as the value for each value
	pairs = nil
	for i := int64(0); i < valueCount; i++ {
		pairs = append(pairs, ardb.IndexActionPair{
			Index:  i,
			Action: ardb.Command(cmd.Set, fmt.Sprintf("foo:%d", i), i),
		})
	}
	replies, err = cluster.DoForAll(pairs)
	require.NoError(err)
	require.Len(replies, valueCount)
	for _, reply := range replies {
		ok, err := ardb.String(reply, nil)
		require.NoError(err)
		require.Equal("OK", ok)
	}

	// now let's get values, and ensure that the order is correct
	pairs = nil
	for i := int64(0); i < valueCount; i++ {
		pairs = append(pairs, ardb.IndexActionPair{
			Index:  i,
			Action: ardb.Command(cmd.Get, fmt.Sprintf("foo:%d", i)),
		})
	}
	replies, err = cluster.DoForAll(pairs)
	require.NoError(err)
	require.Len(replies, valueCount)
	for i, reply := range replies {
		index, err := ardb.Int64(reply, nil)
		require.NoError(err)
		require.Equal(int64(i), index)
	}

	// let's delete all odd indices using `DoFor`, as a last sanity check
	for i := int64(1); i < valueCount; i += 2 {
		ok, err := ardb.Int(cluster.DoFor(i, ardb.Command(cmd.Delete, fmt.Sprintf("foo:%d", i))))
		require.NoError(err)
		require.Equal(1, ok)
	}

	// now let's get values, and ensure that the odd indices are deleted, and that the order is still correct
	pairs = nil
	for i := int64(0); i < valueCount; i++ {
		pairs = append(pairs, ardb.IndexActionPair{
			Index:  i,
			Action: ardb.Command(cmd.Get, fmt.Sprintf("foo:%d", i)),
		})
	}
	replies, err = cluster.DoForAll(pairs)
	require.NoError(err)
	require.Len(replies, valueCount)
	for i, reply := range replies {
		index, err := ardb.Int64(reply, nil)
		if i%2 == 1 {
			require.Equal(ardb.ErrNil, err)
			continue
		}
		require.NoError(err)
		require.Equal(int64(i), index)
	}
}
