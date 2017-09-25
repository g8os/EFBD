// +build !go1.9

package client

import (
	"context"
	"sync"

	log "github.com/Sirupsen/logrus"
	pb "github.com/zero-os/0-stor/grpc_store"
)

type CheckStatus pb.CheckResponse_Status

var (
	CheckStatusOk        = CheckStatus(pb.CheckResponse_ok)
	CheckStatusCorrupted = CheckStatus(pb.CheckResponse_corrupted)
	CheckStatusMissing   = CheckStatus(pb.CheckResponse_missing)
)

func (c *Client) Check(key []byte) (CheckStatus, error) {
	meta, err := c.metaCli.Get(string(key))
	if err != nil {
		log.Errorf("fail to get metadata for check: %v", err)
		return CheckStatusCorrupted, err
	}

	// create a map of chunk per shards, so we can ask all the check for a specific shards in one call
	idsPerShard := make(map[string][][]byte, len(c.policy.DataShards))
	for _, chunk := range meta.Chunks {
		for _, shard := range chunk.Shards {
			if _, ok := idsPerShard[shard]; !ok {
				idsPerShard[shard] = [][]byte{chunk.Key}
			} else {
				idsPerShard[shard] = append(idsPerShard[shard], chunk.Key)
			}
		}
	}

	var (
		cErr  = make(chan error, len(idsPerShard))
		cDone = make(chan struct{})
		// if one block is corrupted, we send a signal on that channel.
		// since we don't care to know what block is corrupted,
		// as soon as something is received on this channel, we can say the file
		// is corrupted
		cStatus     = make(chan CheckStatus, len(idsPerShard))
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)

	// this is called as soon as we know if one block is corrupted
	defer cancel()

	wg.Add(len(idsPerShard))
	for shard, ids := range idsPerShard {
		go func(ctx context.Context, shard string, ids [][]byte, cStatus chan<- CheckStatus, cErr chan<- error) {
			defer wg.Done()

			store, err := c.getStor(shard)
			if err != nil {
				log.Errorf("error getting client store for shard %s: %v", shard, err)
				cErr <- err
				return
			}

			select {
			case <-ctx.Done():
				// in case we already know something is corrupted, we don't need
				// to check other blokcs
				return

			default:
				checks, err := store.ObjectsCheck(ids)
				if err != nil {
					log.Errorf("error getting check status on shard %s: %v", shard, err)
					cErr <- err
					return
				}

				for _, check := range checks {
					status := CheckStatus(check.Status)
					if status != CheckStatusOk {
						// signal we found a corrupted or missing block
						cStatus <- status
					}
				}
			}
		}(ctx, shard, ids, cStatus, cErr)
	}

	go func() {
		wg.Wait()
		cDone <- struct{}{}
	}()

	select {
	case err := <-cErr:
		return CheckStatusCorrupted, err
	case <-cDone:
		// all is good
		return CheckStatusOk, nil
	case state := <-cStatus:
		// something is wrong
		return state, nil
	}
}
