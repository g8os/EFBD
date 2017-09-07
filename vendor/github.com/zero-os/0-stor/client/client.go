package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"

	"github.com/zero-os/0-stor/client/lib"

	"github.com/minio/blake2b-simd"
	"github.com/zero-os/0-stor/client/lib/distribution"
	"github.com/zero-os/0-stor/client/lib/encrypt"
	"github.com/zero-os/0-stor/client/stor"

	"github.com/golang/snappy"
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/client/lib/chunker"
	"github.com/zero-os/0-stor/client/meta"

	log "github.com/Sirupsen/logrus"
)

var (
	errWriteFChunkerOnly    = errors.New("WriteF only support chunker as first pipe")
	errReadFChunkerOnly     = errors.New("ReadF only support chunker as first pipe")
	errNoDataShardAvailable = errors.New("no more data shard available")
)

var _ (itsyouonline.IYOClient) = (*Client)(nil) // build time check that we implement itsyouonline.IYOClient interface

// Client defines 0-stor client
type Client struct {
	policy   Policy
	iyoToken string

	storClients   map[string]stor.Client
	muStorClients sync.Mutex

	metaCli *meta.Client
	iyoCl   itsyouonline.IYOClient
}

// New creates new client from the given config
func New(policy Policy) (*Client, error) {

	iyoCl := itsyouonline.NewClient(policy.Organization, policy.IYOAppID, policy.IYOSecret)

	return newClient(policy, iyoCl)
}

func newClient(policy Policy, iyoCl itsyouonline.IYOClient) (*Client, error) {
	iyoToken, err := iyoCl.CreateJWT(policy.Namespace, itsyouonline.Permission{
		Write: true,
		Read:  true,
	})
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	client := Client{
		policy:      policy,
		iyoToken:    iyoToken,
		iyoCl:       iyoCl,
		storClients: make(map[string]stor.Client, len(policy.DataShards)),
	}

	// instanciate stor client for each shards.
	for _, shard := range policy.DataShards {
		cl, err := stor.NewClientWithToken(&stor.Config{
			IyoClientID: policy.IYOAppID,
			IyoSecret:   policy.IYOSecret,
			Protocol:    policy.Protocol,
			Shard:       shard,
		}, policy.Organization, policy.Namespace, iyoToken)
		if err != nil {
			return nil, err
		}
		client.storClients[shard] = cl
	}

	// instanciate meta client
	if len(policy.MetaShards) > 0 {
		// meta client
		metaCli, err := meta.NewClient(policy.MetaShards)
		if err != nil {
			return nil, err
		}
		client.metaCli = metaCli
	}

	return &client, nil
}

// Close the client
func (c *Client) Close() error {
	if c.metaCli != nil {
		c.metaCli.Close()
	}

	for shard, cl := range c.storClients {
		closer, ok := cl.(io.Closer)
		if ok {
			if err := closer.Close(); err != nil {
				log.Errorf("Error closing stor client to %v", shard)
			}
		}
	}
	return nil
}

// Write write the value to the the 0-stors configured by the client policy
func (c *Client) Write(key, value []byte) (*meta.Meta, error) {
	return c.WriteWithMeta(key, value, nil, nil, nil)
}

func (c *Client) WriteF(key []byte, r io.Reader) (*meta.Meta, error) {
	return c.writeFWithMeta(key, r, nil, nil, nil)
}

// WriteWithMeta writes the key-value to the configured pipes.
// Metadata linked list will be build if prevKey is not nil
// prevMeta is optional previous metadata, to be used in case of user already has the prev meta.
// So the client won't need to fetch it back from the metadata server.
// prevKey still need to be set it prevMeta is set
// initialMeta is optional metadata, if user want to set his own initial metadata for example set own epoch
func (c *Client) WriteWithMeta(key, val []byte, prevKey []byte, prevMeta, md *meta.Meta) (*meta.Meta, error) {
	r := bytes.NewReader(val)
	return c.writeFWithMeta(key, r, prevKey, prevMeta, md)
}

func (c *Client) WriteFWithMeta(key []byte, r io.Reader, prevKey []byte, prevMeta, md *meta.Meta) (*meta.Meta, error) {
	return c.writeFWithMeta(key, r, prevKey, prevMeta, md)
}

func (c *Client) writeFWithMeta(key []byte, r io.Reader, prevKey []byte, prevMeta, md *meta.Meta) (*meta.Meta, error) {
	var (
		blockSize int
		err       error
		aesgm     encrypt.EncrypterDecrypter
		blakeH    = blake2b.New256()
	)

	if c.policy.Encrypt {
		aesgm, err = encrypt.NewEncrypterDecrypter(encrypt.Config{PrivKey: c.policy.EncryptKey, Type: encrypt.TypeAESGCM})
		if err != nil {
			return nil, err
		}
	}

	if len(prevKey) > 0 && prevMeta == nil {
		// get the prev meta now than later
		// to avoid making processing and then
		// just found that prev meta is invalid
		prevMeta, err = c.metaCli.Get(string(prevKey))
		if err != nil {
			return nil, err
		}
	}

	// create new metadata if not given
	if md == nil {
		md = meta.New(key)
	}

	// define the block size to use
	// if policy block size is set to 0:
	//		 we read all content of r, get the size of the data
	// 		 and configuer the chuner with this size, so there is going to be only one chunk
	// else use the block size from the policy
	if c.policy.BlockSize <= 0 {
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		blockSize = len(b)
		r = bytes.NewReader(b)
	} else {
		blockSize = c.policy.BlockSize
	}

	rd := chunker.NewReader(r, chunker.Config{ChunkSize: blockSize})

	var usedShards []string
	for rd.Next() {
		block := rd.Value()

		blakeH.Reset()
		blakeH.Write(block)
		hashed := blakeH.Sum(nil)

		chunkKey := hashed[:]
		chunk := &meta.Chunk{Key: chunkKey}

		if c.policy.Encrypt {
			block, err = aesgm.Encrypt(block)
			md.EncrKey = []byte(c.policy.EncryptKey)
			chunk.Size = uint64(len(block))
		}

		if c.policy.Compress {
			block = snappy.Encode(nil, block)
			chunk.Size = uint64(len(block))
		}

		if c.policy.ReplicationMaxSize > 0 && len(block) <= c.policy.ReplicationMaxSize {
			usedShards, err = c.replicateWrite(chunkKey, block, []string{})
			if err != nil {
				return nil, err
			}
			chunk.Size = uint64(len(block))

		} else if c.policy.DistributionNr > 0 && c.policy.DistributionRedundancy > 0 {
			usedShards, _, err = c.distributeWrite(chunkKey, block, []string{})
			if err != nil {
				return nil, err
			}
			chunk.Size = uint64(len(block))
		} else {
			shard, err := c.writeRandom(chunkKey, block, []string{})
			if err != nil {
				return nil, err
			}
			usedShards = []string{shard}
			chunk.Size = uint64(len(block))
		}

		chunk.Shards = usedShards
		md.Chunks = append(md.Chunks, chunk)
	}

	err = c.linkMeta(md, prevMeta, key, prevKey)
	if err != nil {
		return md, err
	}

	return md, nil
}

// Read reads value with given key from the 0-stors configured by the client policy
// it will first try to get the metadata associated with key from the Metadata servers
func (c *Client) Read(key []byte) ([]byte, error) {
	md, err := c.metaCli.Get(string(key))
	if err != nil {
		return nil, err
	}

	w := &bytes.Buffer{}
	err = c.readFWithMeta(md, w)
	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

// ReadF similar as Read but write the data to w instead of returning a slice of bytes
func (c *Client) ReadF(key []byte, w io.Writer) error {
	md, err := c.metaCli.Get(string(key))
	if err != nil {
		return err
	}
	return c.readFWithMeta(md, w)

}

// ReadWithMeta reads the value described by md
func (c *Client) ReadWithMeta(md *meta.Meta) ([]byte, error) {
	w := &bytes.Buffer{}
	err := c.readFWithMeta(md, w)
	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func (c *Client) readFWithMeta(md *meta.Meta, w io.Writer) error {
	var (
		aesgm encrypt.EncrypterDecrypter
		block []byte
		err   error
	)

	if c.policy.Encrypt {
		aesgm, err = encrypt.NewEncrypterDecrypter(encrypt.Config{PrivKey: c.policy.EncryptKey, Type: encrypt.TypeAESGCM})
		if err != nil {
			return err
		}
	}

	for _, chunk := range md.Chunks {

		if c.policy.ReplicationMaxSize > 0 && chunk.Size <= uint64(c.policy.ReplicationMaxSize) {
			block, err = c.replicateRead(chunk.Key, chunk.Shards)
			if err != nil {
				return err
			}

		} else if c.policy.DistributionNr > 0 && c.policy.DistributionRedundancy > 0 {
			block, err = c.distributeRead(chunk.Key, int(chunk.Size), chunk.Shards)
			if err != nil {
				return err
			}
		} else {
			if len(chunk.Shards) <= 0 {
				panic("metadata corrupted, can't have a chunk without shard")
			}
			block, err = c.read(chunk.Key, chunk.Shards[0])
			if err != nil {
				return err
			}
		}

		if c.policy.Compress {
			block, err = snappy.Decode(nil, block)
			if err != nil {
				return err
			}
		}

		if c.policy.Encrypt {
			block, err = aesgm.Decrypt(block)
			if err != nil {
				return err
			}
		}

		n, err := w.Write(block)
		if err != nil {
			return err
		}
		if n != len(block) {
			panic("couldn't write to Writer")
		}
	}

	return nil
}

func (c *Client) replicateWrite(key, value []byte, referenceList []string) ([]string, error) {

	if c.policy.ReplicationNr <= 2 {
		return nil, fmt.Errorf("replication number can't be lower then 2")
	}

	type Job struct {
		client stor.Client
		shard  string
	}

	var (
		usedShards = []string{}
		okShards   = make([]string, 0, c.policy.ReplicationNr)
		wg         sync.WaitGroup
		mu         sync.Mutex
		shardErr   = &lib.ShardError{}
		cJob       = make(chan *Job)
	)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for job := range cJob {
			wg.Add(1)

			go func(job *Job) {
				defer wg.Done()

				_, err := job.client.ObjectCreate(key, value, referenceList)
				if err != nil {
					log.Errorf("replication write: error writing to store %s: %v", job.shard, err)
					shardErr.Add([]string{job.shard}, lib.ShardType0Stor, err, 0)
					return
				}
				mu.Lock()
				okShards = append(okShards, job.shard)
				mu.Unlock()
			}(job)
		}
	}()

	for i := 0; i < c.policy.ReplicationNr; i++ {
		cl, shard, err := c.getRandomStor(usedShards)
		if err != nil {
			return nil, err
		}

		cJob <- &Job{
			client: cl,
			shard:  shard,
		}
		usedShards = append(usedShards, shard)
	}

	close(cJob)
	wg.Wait()

	if len(okShards) < c.policy.ReplicationNr {
		// missing some replication, try to send sequentially to remaining store
		for len(okShards) < c.policy.ReplicationNr {
			cl, shard, err := c.getRandomStor(usedShards)
			if err != nil {
				// mean we don't have anymore store available
				if err == errNoDataShardAvailable {
					return usedShards, fmt.Errorf("coudn't replicate data to enough 0stor server, only %d succeeded, %d required", len(okShards), c.policy.ReplicationNr)
				}
				return nil, err
			}

			_, err = cl.ObjectCreate(key, value, referenceList)
			if err != nil {
				log.Errorf("replication write: error writing to store %s: %v", shard, err)
				shardErr.Add([]string{shard}, lib.ShardType0Stor, err, 0)
				usedShards = append(usedShards, shard)
				continue
			}

			okShards = append(okShards, shard)
			usedShards = append(usedShards, shard)
		}
	}

	// if still not enough, return error, we can't do anything more
	if len(okShards) < c.policy.ReplicationNr {
		return usedShards, fmt.Errorf("coudn't replicate data to enough 0stor server, only %d succeeded, %d required", len(okShards), c.policy.ReplicationNr)
	}

	return usedShards, nil
}

func (c *Client) replicateRead(key []byte, shards []string) ([]byte, error) {
	wg := sync.WaitGroup{}
	cVal := make(chan []byte)
	cAllDone := make(chan struct{})
	cQuit := make(chan struct{})

	// start a gorountine to all possible shard
	// the first stor to respond send the value received to cVal
	// As soon as something is received into cVal, I close cQuit, so all other rountine should exit
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()

			cl, err := c.getStor(shard)
			if err != nil {
				log.Warningf("replication read, error getting client for %s: %v", shard, err)
				return
			}

			select {
			case <-cQuit:
				return
			default:
				obj, err := cl.ObjectGet(key)
				if err != nil {
					log.Warningf("replication read, error reading from %s: %v", shard, err)
					return
				}
				cVal <- obj.Value
			}
		}(shard)
	}

	// wait for all gorountine to exit
	go func() {
		wg.Wait()
		close(cAllDone)
	}()

	select {
	case <-cAllDone:
		// if we recevie this before the value, it means we couln't get the data back
		// from any store
		close(cQuit)
		return nil, fmt.Errorf("can't find a valid replication of the object")
	case val := <-cVal:
		close(cQuit)
		return val, nil
	}
}

func (c *Client) distributeWrite(key, value []byte, referenceList []string) ([]string, uint64, error) {

	encoder, err := distribution.NewEncoder(c.policy.DistributionNr, c.policy.DistributionRedundancy)
	if err != nil {
		return nil, 0, err
	}

	parts, err := encoder.Encode(value)
	if err != nil {
		return nil, 0, err
	}

	type Job struct {
		client stor.Client
		part   []byte
		shard  string
	}

	var (
		cJob       = make(chan *Job)
		usedShards = make([]string, 0, len(parts))
		size       = uint64(0)
		shardErr   = &lib.ShardError{}
		wg         sync.WaitGroup
	)

	wg.Add(1)
	go func(cJob <-chan *Job) {
		defer wg.Done()
		// gorountine receive work from channel
		// each work object receive start a new goroutine that write the part to the store
		for job := range cJob {
			wg.Add(1)

			go func(job *Job) {
				defer wg.Done()
				_, err := job.client.ObjectCreate(key, job.part, referenceList)
				if err != nil {
					log.Errorf("error writing to stor: %v", err)
					shardErr.Add([]string{job.shard}, lib.ShardType0Stor, err, 0)
				}
			}(job)
		}
	}(cJob)

	for i, part := range parts {
		cl, shard, err := c.getRandomStor(usedShards)
		if err != nil {
			if err == errNoDataShardAvailable {
				return nil, 0, shardErr
			}
			shardErr.Add([]string{shard}, lib.ShardType0Stor, err, 0)
			continue
		}

		cJob <- &Job{
			client: cl,
			shard:  shard,
			part:   part,
		}

		usedShards = append(usedShards, shard)
		if i < c.policy.DistributionNr {
			size += uint64(len(part))
		}
	}
	// close job channel, this will allow job consuming routine to exit
	close(cJob)

	wg.Wait()

	if !shardErr.Nil() {
		log.Errorf("error distributin write: %v", shardErr)
		return usedShards, size, shardErr
	}

	return usedShards, size, nil
}

func (c *Client) distributeRead(key []byte, originalSize int, shards []string) ([]byte, error) {
	parts := make([][]byte, len(shards))

	dec, err := distribution.NewDecoder(c.policy.DistributionNr, c.policy.DistributionRedundancy)
	if err != nil {
		return nil, err
	}

	var (
		wg       = sync.WaitGroup{}
		shardErr = &lib.ShardError{}
	)

	wg.Add(len(shards))
	for i, shard := range shards {
		go func(i int, shard string) {
			defer wg.Done()

			cl, err := c.getStor(shard)
			if err != nil {
				shardErr.Add([]string{shard}, lib.ShardType0Stor, err, 0)
				return
			}

			obj, err := cl.ObjectGet(key)
			if err != nil {
				log.Errorf("error read %s from stor(%s): %v", fmt.Sprintf("%x", key), shard, err)
				shardErr.Add([]string{shard}, lib.ShardType0Stor, err, 0)
				return
			}
			parts[i] = obj.Value
		}(i, shard)
	}

	wg.Wait()

	if !shardErr.Nil() {
		return nil, shardErr
	}

	return dec.Decode(parts, originalSize)
}

func (c *Client) writeRandom(key, value []byte, referenceList []string) (string, error) {
	triedShards := []string{}

	for {
		cl, shard, err := c.getRandomStor(triedShards)
		if err != nil {
			return "", err
		}

		triedShards = append(triedShards, shard)

		_, err = cl.ObjectCreate(key, value, referenceList)
		if err == nil {
			return shard, nil
		}
		log.Error(err)
	}
}

func (c *Client) read(key []byte, shard string) ([]byte, error) {
	cl, err := c.getStor(shard)
	if err != nil {
		return nil, err
	}

	obj, err := cl.ObjectGet(key)
	if err != nil {
		return nil, err
	}
	return obj.Value, nil
}

func (c *Client) linkMeta(curMd, prevMd *meta.Meta, curKey, prevKey []byte) error {
	if len(prevKey) == 0 {
		return c.metaCli.Put(string(curKey), curMd)
	}

	// point next key of previous meta to new meta
	prevMd.Next = curKey

	// point prev key of new meta to previous one
	curMd.Previous = prevKey

	// update prev meta
	if err := c.metaCli.Put(string(prevKey), prevMd); err != nil {
		return err
	}

	// update new meta
	return c.metaCli.Put(string(curKey), curMd)
}

func (c *Client) getRandomStor(except []string) (stor.Client, string, error) {
	isIn := func(target string, list []string) bool {
		for _, x := range list {
			if target == x {
				return true
			}
		}
		return false
	}

	c.muStorClients.Lock()
	defer c.muStorClients.Unlock()

	possibleShards := []string{}
	for _, shard := range c.policy.DataShards {
		if !isIn(shard, except) {
			possibleShards = append(possibleShards, shard)
		}
	}

	var shard string
	if len(possibleShards) <= 0 {
		return nil, "", errNoDataShardAvailable
	} else if len(possibleShards) == 1 {
		shard = possibleShards[0]
	} else {
		shard = possibleShards[rand.Intn(len(possibleShards)-1)]
	}

	// TODO: find a way to invalidate some client if an error occurs with it

	// first check if we don't already have a client to this shard loaded
	cl, ok := c.storClients[shard]
	if ok {
		return cl, shard, nil
	}

	// if not create the client and put in cache
	cl, err := stor.NewClientWithToken(&stor.Config{
		IyoClientID: c.policy.IYOAppID,
		IyoSecret:   c.policy.IYOSecret,
		Protocol:    c.policy.Protocol,
		Shard:       shard,
	}, c.policy.Organization, c.policy.Namespace, c.iyoToken)
	if err != nil {
		return nil, shard, err
	}
	c.storClients[shard] = cl

	return cl, shard, err
}

func (c *Client) getStor(shard string) (stor.Client, error) {
	c.muStorClients.Lock()
	defer c.muStorClients.Unlock()

	// first check if we don't already have a client to this shard loaded
	cl, ok := c.storClients[shard]
	if ok {
		return cl, nil
	}

	// if not create the client and put in cache
	cl, err := stor.NewClientWithToken(&stor.Config{
		IyoClientID: c.policy.IYOAppID,
		IyoSecret:   c.policy.IYOSecret,
		Protocol:    c.policy.Protocol,
		Shard:       shard,
	}, c.policy.Organization, c.policy.Namespace, c.iyoToken)
	if err != nil {
		return nil, err
	}
	c.storClients[shard] = cl

	return cl, err
}

func (c *Client) CreateJWT(namespace string, perm itsyouonline.Permission) (string, error) {
	return c.iyoCl.CreateJWT(namespace, perm)
}
func (c *Client) CreateNamespace(namespace string) error {
	return c.iyoCl.CreateNamespace(namespace)
}
func (c *Client) DeleteNamespace(namespace string) error {
	return c.iyoCl.DeleteNamespace(namespace)
}
func (c *Client) GivePermission(namespace, userID string, perm itsyouonline.Permission) error {
	return c.iyoCl.GivePermission(namespace, userID, perm)
}
func (c *Client) RemovePermission(namespace, userID string, perm itsyouonline.Permission) error {
	return c.iyoCl.RemovePermission(namespace, userID, perm)
}
func (c *Client) GetPermission(namespace, userID string) (itsyouonline.Permission, error) {
	return c.iyoCl.GetPermission(namespace, userID)
}
