package storage

import (
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/nbd/ardb"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// LoadTlogMetadata loads a given vdisk's tlog metadata from the given ARDB storage cluster.
func LoadTlogMetadata(vdiskID string, cluster ardb.StorageCluster) (TlogMetadata, error) {
	var md TlogMetadata
	if cluster == nil {
		return md, ErrClusterNotDefined
	}

	var err error
	key := tlogMetadataKey(vdiskID)

	md.LastFlushedSequence, err = ardb.OptUint64(cluster.Do(
		ardb.Command(command.HashGet, key, tlogMetadataLastFlushedSequenceField)))
	return md, err
}

// StoreTlogMetadata stores a given vdisk's tlog metadata on the given ARDB storage cluster.
func StoreTlogMetadata(vdiskID string, cluster ardb.StorageCluster, md TlogMetadata) error {
	key := tlogMetadataKey(vdiskID)
	_, err := cluster.Do(
		ardb.Command(command.HashSet,
			key, tlogMetadataLastFlushedSequenceField,
			md.LastFlushedSequence))
	return err
}

// TlogMetadata defines all tlog-related metadata.
type TlogMetadata struct {
	LastFlushedSequence uint64
}

// tlogMetadataKey returns the key of the ARDB hashmap,
// which contains all the metadata stored for a tlog storage.
func tlogMetadataKey(vdiskID string) string {
	return tlogMetadataKeyPrefix + vdiskID
}

// copyTlogMetadata copies tlog metadata
// within the same or between different storage clusters.
func copyTlogMetadata(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) error {
	if isInterfaceValueNil(targetCluster) {
		return copyTlogMetadataSingleCluster(sourceID, targetID, sourceCluster)
	}

	return copyTlogMetadataBetweenClusters(sourceID, targetID, sourceCluster, targetCluster)
}

func copyTlogMetadataSingleCluster(sourceID, targetID string, cluster ardb.StorageCluster) error {
	sourceKey := tlogMetadataKey(sourceID)
	targetkey := tlogMetadataKey(targetID)

	log.Debugf("copy tlog metadata from %s to %s on same cluster",
		sourceKey, targetkey)

	action := ardb.Script(0, copyTlogMetadataSameConnScript,
		nil, sourceKey, targetkey)

	return ardb.Error(cluster.Do(action))
}

func copyTlogMetadataBetweenClusters(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) error {
	log.Debugf("load tlog metadata from source cluster for %s", sourceID)
	metadata, err := LoadTlogMetadata(sourceID, sourceCluster)
	if err != nil {
		return errors.Wrapf(err,
			"couldn't deserialize source tlog metadata for %s", sourceID)
	}

	log.Debugf("store tlog metadata on target cluster for %s", targetID)
	err = StoreTlogMetadata(targetID, targetCluster, metadata)
	if err != nil {
		return errors.Wrapf(err,
			"couldn't serialize destination tlog metadata for %s", targetID)
	}

	return nil
}

const copyTlogMetadataSameConnScript = `
	local source = ARGV[1]
	local dest = ARGV[2]
	
	if redis.call("EXISTS", source) == 0 then
		return 0
	end
	
	if redis.call("EXISTS", dest) == 1 then
		redis.call("DEL", dest)
	end
	
	redis.call("RESTORE", dest, 0, redis.call("DUMP", source))
	return 1
`

const (
	tlogMetadataKeyPrefix                = "tlog:"
	tlogMetadataLastFlushedSequenceField = "lfseq"
)
