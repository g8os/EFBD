package client

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/zero-os/0-stor/client/meta"
	pb "github.com/zero-os/0-stor/grpc_store"
)

var (
	// ErrAllReplicateCorrupted is returned when a  All the replicate are corrupted
	ErrAllReplicateCorrupted = fmt.Errorf("All the replicate are corrupted, repair impossible")
	// ErrRepairSupport is returned when a block is not using replication or distribution
	ErrRepairSupport = fmt.Errorf("block is not using replication or distribution, repair impossible")
)

// Repair repairs a broken file.
// If the file is distributed and the ammout of corrupted chunks is acceptable,
// we recreate the missing chunks.
// Id the file is replicated and we still have one valid replicate, we create the missing replicate
// till we reach the replication number configured in the policy
// if the file as not been distributed or replicated, we can't repair it
func (c *Client) Repair(key []byte) error {
	log.Infof("Start repair of %x", key)
	meta, err := c.metaCli.Get(string(key))
	if err != nil {
		log.Errorf("repair %x, error getting metadata :%v", key, err)
		return err
	}

	for _, chunk := range meta.Chunks {
		switch {
		case c.policy.ReplicationEnabled(int(chunk.Size)):
			chunk.Shards, err = c.repairReplicatedChunk(chunk)
			if err != nil {
				return err
			}
		case c.policy.DistributionEnabled():
			chunk.Shards, _, err = c.repairDistributedChunk(chunk)
			if err != nil {
				return err
			}
		default:
			return ErrRepairSupport
		}
	}

	if err := c.metaCli.Put(string(meta.Key), meta); err != nil {
		log.Errorf("error writing metadta after repair: %v", err)
		return err
	}

	return nil
}

func (c *Client) repairDistributedChunk(chunk *meta.Chunk) ([]string, uint64, error) {

	// distributeRead already take case of reconstrtucting the data if we have missing parts
	obj, err := c.distributeRead(chunk.Key, int(chunk.Size), chunk.Shards)
	if err != nil {
		log.Errorf("repair distribution error: %v", err)
		return nil, 0, err
	}

	// we just rewrite the data to new shards
	return c.distributeWrite(chunk.Key, obj.Value, obj.ReferenceList)
}

func (c *Client) repairReplicatedChunk(chunk *meta.Chunk) ([]string, error) {
	// check which replicate are corrupted
	// create a map of chunk per shards, so we can ask all the check for a specific shards in one call
	var (
		corruptedShards []string
		validShards     []string
		cCorrupted      = make(chan string)
		wg              sync.WaitGroup
	)

	// get all corrupted shards
	wg.Add(len(chunk.Shards))
	for _, shard := range chunk.Shards {
		go func(shard string, cCorrupted chan string) {
			defer wg.Done()

			store, err := c.getStor(shard)
			if err != nil {
				cCorrupted <- shard
				return
			}

			status, err := store.ObjectsCheck([][]byte{chunk.Key})
			if err != nil {
				cCorrupted <- shard
				return
			}
			if len(status) < 1 || CheckStatus(status[0].Status) != CheckStatusOk {
				cCorrupted <- shard
				return
			}

		}(shard, cCorrupted)
	}

	go func() {
		wg.Wait()
		close(cCorrupted)
	}()

	// received all corrupted shards from the gorountines
	for corruptedShard := range cCorrupted {
		corruptedShards = append(corruptedShards, corruptedShard)
	}

	// create list of valid Shards
	validShards = make([]string, 0, len(chunk.Shards)-len(corruptedShards))
	for _, shard := range chunk.Shards {
		if !isIn(shard, corruptedShards) {
			validShards = append(validShards, shard)
		}
	}
	if len(validShards) <= 0 {
		return nil, ErrAllReplicateCorrupted
	}

	// read valid block from one of the valid shard
	var validObj *pb.Object
	for _, shard := range validShards {
		store, err := c.getStor(shard)
		if err != nil {
			continue
		}
		obj, err := store.ObjectGet(chunk.Key)
		if err != nil {
			continue
		}
		validObj = obj
		break
	}

	// could be that in between we couldn't get any valid block anymore
	if validObj == nil {
		return nil, ErrAllReplicateCorrupted
	}

	// for all corrupted shard, we replace it
	newShards := []string{}
	var err error
	for i := 0; i < len(corruptedShards); i++ {
		err = nil
		for err != nil {
			store, shard, err := c.getRandomStor(corruptedShards)
			if err != nil {
				// we could endup here, if we tried all available shard, and coulnd't write on any of them.
				// then it nornal to return an error cause Repair failed
				return nil, err
			}

			err = store.ObjectCreate(chunk.Key, validObj.Value, validObj.ReferenceList)
			if err != nil {
				// coulnd't write data on the 0-stor return by getRandomStor
				// we add this shard to the corrupted list
				// so in the next iteration getRandomStor doesn't propose this shard anymore
				corruptedShards = append(corruptedShards, shard)
				continue
			}
			newShards = append(newShards, shard)
		}

	}

	return append(validShards, newShards...), nil
}

func isIn(target string, list []string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}
