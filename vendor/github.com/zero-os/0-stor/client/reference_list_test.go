package client

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/zero-os/0-stor/client/meta/embedserver"
	"github.com/zero-os/0-stor/server/db"

	"github.com/stretchr/testify/require"
)

func TestReferenceList(t *testing.T) {
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
		Organization: "testorg",
		Namespace:    "namespace1",
		DataShards:   shards,
		MetaShards:   []string{etcd.ListenAddr()},
		IYOAppID:     "id",
		IYOSecret:    "secret",
	}

	tt := []struct {
		name      string
		BlockSize int

		ReplicationNr      int
		ReplicationMaxSize int

		DistributionNr         int
		DistributionRedundancy int
	}{
		{
			name:                   "distribution",
			ReplicationMaxSize:     1, //force to use distribution over replication
			DistributionNr:         3,
			DistributionRedundancy: 1,
		},
		/*{
			name:                   "chunks-distribution",
			BlockSize:              1024,
			ReplicationMaxSize:     1, //force to use distribution over replication
			DistributionNr:         3,
			DistributionRedundancy: 1,
		},
		{
			name:               "chunks-replication",
			BlockSize:          1024,
			ReplicationNr:      len(shards),
			ReplicationMaxSize: 1024 * 1024,
		},*/
	}

	// initialize test data
	key := []byte("testkey")
	data := make([]byte, 1024*4)
	_, err = rand.Read(data)
	require.NoError(t, err, "fail to read random data")

	// initialize ref list
	maxAllowedRefList := db.RefIDCount

	allRefList := make([]string, 0, maxAllowedRefList+1)
	for i := 0; i < maxAllowedRefList+1; i++ {
		allRefList = append(allRefList, fmt.Sprintf("vdisk-%v", i))
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			policy.BlockSize = tc.BlockSize
			policy.DistributionNr = tc.DistributionNr
			policy.DistributionRedundancy = tc.DistributionRedundancy
			policy.ReplicationNr = tc.ReplicationNr
			policy.ReplicationMaxSize = tc.ReplicationMaxSize

			c, err := getTestClient(policy)
			require.NoError(t, err, "fail to create client")

			// write data to the store with too-big refList
			_, err = c.Write(key, data, allRefList)
			require.Error(t, err)

			// write data to the store with proper number of refList
			refList := allRefList[:maxAllowedRefList]
			_, err = c.Write(key, data, refList)
			require.NoError(t, err, "fail to write data to the store")

			// check
			_, refListRead, err := c.Read(key)
			require.Equal(t, refList, refListRead)

			// remove reference list
			removeIndex := maxAllowedRefList / 2
			err = c.RemoveReferenceList(key, allRefList[removeIndex:])
			require.NoError(t, err)

			_, refListRead, err = c.Read(key)
			require.NoError(t, err)
			require.Equal(t, allRefList[:removeIndex], refListRead)

			// append some of it
			appendIndex := removeIndex + (removeIndex / 2)
			err = c.AppendReferenceList(key, allRefList[removeIndex:appendIndex])
			require.NoError(t, err)

			_, refListRead, err = c.Read(key)
			require.NoError(t, err)
			require.Equal(t, allRefList[:appendIndex], refListRead)

			// set again to full value
			err = c.SetReferenceList(key, allRefList[:maxAllowedRefList])
			require.NoError(t, err)

			_, refListRead, err = c.Read(key)
			require.NoError(t, err)
			require.Equal(t, allRefList[:maxAllowedRefList], refListRead)

		})
	}

}
