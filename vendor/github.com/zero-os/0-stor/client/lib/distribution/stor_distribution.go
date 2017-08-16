package distribution

import (
	"fmt"
	"sync"

	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/lib/hash"
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/stor"
)

// StorDistributor defines distributor that use 0-stor rest/grpc clients
// as Writers.
// It create one stor.Client for each shard
type StorDistributor struct {
	enc         *Encoder
	hasher      *hash.Hasher
	storClients []stor.Client
	shards      []string
	metaCli     *meta.Client
	w           block.Writer
}

// NewStorDistributor creates new StorDistributor
func NewStorDistributor(w block.Writer, conf Config, shards, metaShards []string, proto,
	org, namespace, iyoAppID, iyoAppSecret string) (*StorDistributor, error) {

	if len(shards) < conf.NumPieces() {
		return nil, fmt.Errorf("invalid number of shards=%v, expected=%v", len(shards), conf.NumPieces())
	}

	// stor clients
	storClients, err := createStorClients(conf, shards, proto, org, namespace, iyoAppID, iyoAppSecret)
	if err != nil {
		return nil, err
	}

	// encoder
	enc, err := NewEncoder(conf.Data, conf.Parity)
	if err != nil {
		return nil, err
	}

	// hasher
	hasher, err := hash.NewHasher(hash.Config{Type: hash.TypeBlake2})
	if err != nil {
		return nil, err
	}

	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	return &StorDistributor{
		storClients: storClients,
		enc:         enc,
		hasher:      hasher,
		shards:      shards,
		metaCli:     metaCli,
		w:           w,
	}, nil
}

// Write implements io.Writer
func (sd StorDistributor) Write(data []byte) (int, error) {
	key := sd.hasher.Hash(data)
	encoded, err := sd.enc.Encode(data)
	if err != nil {
		return 0, err
	}

	for i, piece := range encoded {
		if _, err = sd.storClients[i].ObjectCreate(key, piece, nil); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

// WriteBlock implements block.Writer interface.
// it also writes to the metadata server
func (sd StorDistributor) WriteBlock(key, value []byte, md *meta.Meta) (*meta.Meta, error) {
	hashedKey := sd.hasher.Hash(value)
	encoded, err := sd.enc.Encode(value)
	if err != nil {
		return md, err
	}

	var (
		errs []error
		cErr = make(chan error)
		wg   sync.WaitGroup
	)

	wg.Add(len(encoded))
	for i, piece := range encoded {
		go func(idx int, data []byte, cErr chan error) {
			defer wg.Done()
			_, err := sd.storClients[idx].ObjectCreate(hashedKey, data, nil)
			if err != nil {
				cErr <- err
			}
		}(i, piece, cErr)
	}

	go func(cErr chan error) {
		for err := range cErr {
			errs = append(errs, err)
		}
	}(cErr)

	wg.Wait()
	close(cErr)

	if len(errs) > 0 {
		return md, Error{errs: errs}
	}

	if err := sd.updateMeta(md, hashedKey, len(value), sd.shards); err != nil {
		return md, err
	}

	if err := sd.metaCli.Put(string(key), md); err != nil {
		return nil, err
	}

	mdBytes, err := md.Bytes()
	if err != nil {
		return md, err
	}

	return sd.w.WriteBlock(key, mdBytes, md)
}

func (sd StorDistributor) updateMeta(md *meta.Meta, key []byte, size int, shards []string) error {
	if err := md.SetKey(key); err != nil {
		return err
	}
	md.SetSize(uint64(size))
	md.SetEpochNow()
	return md.SetShardSlice(shards)
}

// StorRestorer defines distributor that get the data
// from 0-stor
type StorRestorer struct {
	conf         Config
	proto        string
	dec          *Decoder
	storClients  map[string]stor.Client
	jwtToken     string
	org          string
	namespace    string
	iyoAppID     string
	iyoAppSecret string
	metaCli      *meta.Client
}

// NewStorRestorer creates new StorRestorer
func NewStorRestorer(conf Config, shards, metaShards []string, proto, org, namespace,
	iyoAppID, iyoAppSecret string) (*StorRestorer, error) {

	dec, err := NewDecoder(conf.Data, conf.Parity)
	if err != nil {
		return nil, err
	}

	metaCli, err := meta.NewClient(metaShards)
	if err != nil {
		return nil, err
	}

	sr := &StorRestorer{
		conf:         conf,
		proto:        proto,
		storClients:  make(map[string]stor.Client),
		dec:          dec,
		org:          org,
		namespace:    namespace,
		iyoAppID:     iyoAppID,
		iyoAppSecret: iyoAppSecret,
		metaCli:      metaCli,
	}
	return sr, sr.createStorClients(shards)
}

// ReadBlock implements block.Reader
func (sr StorRestorer) ReadBlock(metaKey []byte) ([]byte, error) {
	md, err := sr.metaCli.Get(string(metaKey))
	if err != nil {
		return nil, err
	}

	key, err := md.Key()
	if err != nil {
		return nil, err
	}

	shards, err := md.GetShardsSlice()
	if err != nil {
		return nil, err
	}

	var errs []error
	chunks := make([][]byte, sr.dec.k+sr.dec.m)

	// read all chunks from stor.Clients concurrently
	var mux sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(shards))

	for i, v := range shards {
		// start goroutine for each shard
		go func(idx int, shard string) {
			defer wg.Done()

			// get value from each shard
			val, err := func() (val []byte, err error) {
				// get the proper shard
				sc, exists := sr.storClients[shard]
				if !exists {
					err = fmt.Errorf("stor client for %v is not exist. it should never happen", shard)
					return
				}

				// get the object
				obj, err := sc.ObjectGet(key)
				if err != nil {
					return
				}

				// store the value
				val = obj.Value
				return
			}()

			mux.Lock()
			defer mux.Unlock()

			if err != nil {
				errs = append(errs, err)
			} else {
				chunks[idx] = val
			}
		}(i, v)
	}
	wg.Wait()

	if len(errs) > sr.dec.m {
		// it failed for more than number of parity
		return nil, Error{errs: errs}
	}

	// decode
	decoded, err := sr.dec.Decode(chunks, int(md.Size()))
	return decoded, err
}

// create stor clients for given shards
// the created client is stored and used for future use
func (sr *StorRestorer) createStorClients(shards []string) error {
	// create jwt token if needed
	if sr.jwtToken == "" {
		token, err := createJWTToken(sr.conf, sr.org, sr.namespace, sr.iyoAppID, sr.iyoAppSecret)
		if err != nil {
			return err
		}
		sr.jwtToken = token
	}

	// create shards if needed
	for _, shard := range shards {
		if _, exists := sr.storClients[shard]; exists {
			continue
		}

		clients, err := createStorClientsWithToken(sr.conf, []string{shard}, sr.proto,
			sr.org, sr.namespace, sr.jwtToken)
		if err != nil {
			return err
		}
		sr.storClients[shard] = clients[0]
	}
	return nil
}

func createStorClients(conf Config, shards []string, proto, org, namespace, iyoAppID, iyoAppSecret string) ([]stor.Client, error) {
	token, err := createJWTToken(conf, org, namespace, iyoAppID, iyoAppSecret)
	if err != nil {
		return nil, err
	}
	return createStorClientsWithToken(conf, shards, proto, org, namespace, token)
}

func createStorClientsWithToken(conf Config, shards []string, proto, org, namespace, token string) ([]stor.Client, error) {
	var scs []stor.Client

	// create stor clients
	storConf := stor.Config{
		Protocol: proto,
	}
	for _, shard := range shards {
		storConf.Shard = shard
		storClient, err := stor.NewClientWithToken(&storConf, org, namespace, token)
		if err != nil {
			return nil, err
		}
		scs = append(scs, storClient)
	}
	return scs, nil
}

func createJWTToken(conf Config, org, namespace, iyoAppID, iyoAppSecret string) (string, error) {
	if !withIYoCredentials(iyoAppID, iyoAppSecret) {
		return "", nil
	}

	iyoClient := itsyouonline.NewClient(org, iyoAppID, iyoAppSecret)
	return iyoClient.CreateJWT(namespace, itsyouonline.Permission{
		Read:   true,
		Write:  true,
		Delete: true,
	})
}
