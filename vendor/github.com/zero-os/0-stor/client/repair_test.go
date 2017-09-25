package client

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zero-os/0-stor/client/meta/embedserver"
)

func TestRepair(t *testing.T) {

	etcd, err := embedserver.New()
	require.NoError(t, err, "fail to start embebed etcd server")
	defer etcd.Stop()

	servers, serverClean := testGRPCServer(t, 4)
	defer serverClean()

	shards := make([]string, len(servers))
	for i, server := range servers {
		shards[i] = server.Addr()
	}

	policy := Policy{
		Organization:           "testorg",
		Namespace:              "namespace1",
		DataShards:             shards,
		MetaShards:             []string{etcd.ListenAddr()},
		IYOAppID:               "",
		IYOSecret:              "",
		BlockSize:              1024,
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             "cF0BFpIsljOS8UmaP8YRHRX0nBPVRVPw",
		ReplicationNr:          0,
		ReplicationMaxSize:     1, //force to use distribution over replication
		DistributionNr:         3,
		DistributionRedundancy: 1,
	}

	tt := []struct {
		name string

		ReplicationNr      int
		ReplicationMaxSize int

		DistributionNr         int
		DistributionRedundancy int
		repairErr              error
	}{
		{
			name:                   "replication",
			ReplicationNr:          4,
			ReplicationMaxSize:     1024 * 10,
			DistributionNr:         0,
			DistributionRedundancy: 0,
			repairErr:              nil,
		},
		{
			name:                   "distribution",
			ReplicationNr:          0,
			ReplicationMaxSize:     0,
			DistributionNr:         3,
			DistributionRedundancy: 1,
			repairErr:              nil,
		},
		{
			name:                   "no-repair-suport",
			ReplicationNr:          0,
			ReplicationMaxSize:     0,
			DistributionNr:         0,
			DistributionRedundancy: 0,
			repairErr:              ErrRepairSupport,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			policy.DistributionNr = tc.DistributionNr
			policy.DistributionRedundancy = tc.DistributionRedundancy
			policy.ReplicationNr = tc.ReplicationNr
			policy.ReplicationMaxSize = tc.ReplicationMaxSize
			testRepair(t, policy, tc.repairErr)
		})

	}

}

func testRepair(t *testing.T, policy Policy, repairErr error) {
	c, err := getTestClient(policy)
	require.NoError(t, err, "fail to create client")
	defer c.Close()

	data := make([]byte, 1204*10)
	key := make([]byte, 64)

	_, err = rand.Read(data)
	require.NoError(t, err, "fail to read random data")
	_, err = rand.Read(key)
	require.NoError(t, err, "fail to read random key")
	refList := []string{"ref-1", "ref-2"}

	meta, err := c.Write(key, data, refList)
	require.NoError(t, err, "fail write data")

	// Check status is ok after a write
	status, err := c.Check(meta.Key)
	require.NoError(t, err, "fail to check object")
	require.Equal(t, CheckStatusOk, status)

	// corrupt file by removing a block
	store, err := c.getStor(meta.Chunks[0].Shards[0])
	require.NoError(t, err)
	err = store.ObjectDelete(meta.Chunks[0].Key)
	require.NoError(t, err)

	// Check status is corrupted
	status, err = c.Check(meta.Key)
	require.NoError(t, err, "fail to check object")
	require.Equal(t, CheckStatusMissing, status)

	// try to repair
	err = c.Repair(meta.Key)
	if repairErr != nil {
		assert.Error(t, repairErr, err)
	}

	if repairErr == nil {
		require.NoError(t, err)
		// make sure we can read the data again
		readData, readRefList, err := c.Read(meta.Key)
		require.NoError(t, err)
		assert.Equal(t, data, readData, "restored data is not the same as initial data")
		assert.Equal(t, refList, readRefList, "restored reference list is not the same as initial reference list")
	}

}
