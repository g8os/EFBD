package storage

import (
	"fmt"

	"github.com/zero-os/0-Disk/config"
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

// CopyTlogMetadata copies all metadata of a tlog-enabled storage
// from a sourceID to a targetID, within the same cluster or between different clusters.
func CopyTlogMetadata(sourceID, targetID string, sourceClusterCfg config.StorageClusterConfig, targetClusterCfg *config.StorageClusterConfig) error {
	// define whether or not we're copying between different servers.
	if targetClusterCfg == nil {
		targetClusterCfg = &sourceClusterCfg
	}

	// get first available storage server

	metaSourceCfg, err := ardb.FindFirstAvailableServerConfig(sourceClusterCfg)
	if err != nil {
		return err
	}

	sourceCluster, err := ardb.NewUniCluster(metaSourceCfg, nil)
	if err != nil {
		return err
	}

	metaTargetCfg, err := ardb.FindFirstAvailableServerConfig(*targetClusterCfg)
	if err != nil {
		return err
	}

	if metaSourceCfg.Equal(metaTargetCfg) {
		conn, err := ardb.Dial(metaSourceCfg)
		if err != nil {
			return fmt.Errorf("couldn't connect to ardb: %s", err.Error())
		}
		defer conn.Close()

		return copyTlogMetadataSameConnection(sourceID, targetID, sourceCluster)
	}

	targetCluster, err := ardb.NewUniCluster(metaTargetCfg, nil)
	if err != nil {
		return err
	}

	return copyTlogMetadataDifferentConnections(sourceID, targetID, sourceCluster, targetCluster)
}

func copyTlogMetadataDifferentConnections(sourceID, targetID string, sourceCluster, targetCluster ardb.StorageCluster) error {
	metadata, err := LoadTlogMetadata(sourceID, sourceCluster)
	if err != nil {
		return fmt.Errorf(
			"couldn't deserialize source tlog metadata for %s: %v", sourceID, err)
	}

	err = StoreTlogMetadata(targetID, targetCluster, metadata)
	if err != nil {
		return fmt.Errorf(
			"couldn't serialize destination tlog metadata for %s: %v", targetID, err)
	}

	return nil
}

func copyTlogMetadataSameConnection(sourceID, targetID string, cluster ardb.StorageCluster) error {
	log.Infof("dumping tlog metadata of vdisk %q and restoring it as tlog metadata of vdisk %q",
		sourceID, targetID)

	sourceKey, targetKey := tlogMetadataKey(sourceID), tlogMetadataKey(targetID)
	_, err := cluster.Do(ardb.Script(
		0, copyTlogMetadataSameConnScript,
		[]string{targetKey},
		sourceKey, targetKey))
	return err
}

var copyTlogMetadataSameConnScript = `
	local source = ARGV[1]
	local dest = ARGV[2]
	
	if redis.call("EXISTS", source) == 0 then
		return
	end
	
	if redis.call("EXISTS", dest) == 1 then
		redis.call("DEL", dest)
	end
	
	redis.call("RESTORE", dest, 0, redis.call("DUMP", source))
`

const (
	tlogMetadataKeyPrefix                = "tlog:"
	tlogMetadataLastFlushedSequenceField = "lfseq"
)
