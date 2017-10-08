package stor

import (
	"errors"
	"fmt"
	"sync"

	storclient "github.com/zero-os/0-stor/client"
	"github.com/zero-os/0-stor/client/lib/hash"
	"github.com/zero-os/0-stor/client/meta"

	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
)

const (
	capnpBufLen = 4096 * 4
)

var (
	ErrNoFlushedBlock = errors.New("no flushed block")
)

// Config defines the 0-stor client config
type Config struct {
	VdiskID         string
	Organization    string
	Namespace       string
	IyoClientID     string
	IyoSecret       string
	ZeroStorShards  []string
	MetaShards      []string
	DataShardsNum   int
	ParityShardsNum int
	EncryptPrivKey  string
}

// Client defines the 0-stor client
type Client struct {
	vdiskID string

	// 0-stor client
	storClient *storclient.Client

	hasher *hash.Hasher

	// capnp buffer
	capnpBuf []byte

	metaCli *MetaClient
	// first & last metadata key
	firstMetaKey []byte
	lastMetaKey  []byte
	lastSequence uint64

	// etcd key in which we store our last metadata
	// we need to store it so we still know it after restart
	lastMetaEtcdKey  []byte
	firstMetaEtcdKey []byte

	lastMd *meta.Meta

	// refList is 0-stor reference list for this vdisk
	refList []string

	mux      sync.Mutex
	storeNum int
}

// NewClient creates new client from the given config
func NewClient(conf Config) (*Client, error) {
	// 0-stor client
	sc, err := storclient.New(newStorClientConf(conf))
	if err != nil {
		return nil, err
	}

	hasher, err := hash.NewHasher(hash.Config{
		Type: hash.TypeBlake2,
	})
	if err != nil {
		return nil, err
	}

	metaCli, err := NewMetaClient(conf.MetaShards)
	if err != nil {
		return nil, err
	}

	cli := &Client{
		vdiskID:          conf.VdiskID,
		storClient:       sc,
		capnpBuf:         make([]byte, 0, capnpBufLen),
		hasher:           hasher,
		metaCli:          metaCli,
		firstMetaEtcdKey: []byte(fmt.Sprintf("tlog:%v:first_meta", conf.VdiskID)),
		lastMetaEtcdKey:  []byte(fmt.Sprintf("tlog:%v:last_meta", conf.VdiskID)),
		refList:          []string{conf.VdiskID},
	}

	firstMetaKey, err := cli.getFirstMetaKey()
	if err != nil {
		log.Errorf("failed to get first key of `%v`: %v", cli.vdiskID, err)
		return nil, err
	}
	cli.firstMetaKey = firstMetaKey

	if _, err = cli.LoadLastSequence(); err != nil && err != ErrNoFlushedBlock {
		log.Errorf("failed to load last sequence of `%v`: %v", cli.vdiskID, err)
		return nil, err
	}

	return cli, nil
}

// NewClientFromConfigSource creates new client from given config.Source
func NewClientFromConfigSource(confSource config.Source, vdiskID, privKey string,
	dataShards, parityShards int) (*Client, error) {

	conf, err := ConfigFromConfigSource(confSource, vdiskID, privKey, dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	return NewClient(conf)
}

// ProcessStore processes and then stores the data to 0-stor server
func (c *Client) ProcessStore(blocks []*schema.TlogBlock) ([]byte, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	timestamp := tlog.TimeNowTimestamp()
	// encode capnp
	data, err := c.encodeCapnp(blocks, timestamp)
	if err != nil {
		return nil, err
	}

	key := c.hasher.Hash(append([]byte(c.vdiskID), data...))

	// we set our initial meta because
	// we need to set our own timestamp in the 0-stor metadata server
	// in order to have only one epoch for both tlog aggregation and 0-stor metadata
	initialMeta := meta.New(key)
	initialMeta.Epoch = timestamp

	// stor to 0-stor
	lastMd, err := c.storClient.WriteWithMeta(key, data, c.lastMetaKey, c.lastMd, initialMeta, c.refList)
	if err != nil {
		return nil, err
	}
	if lastMd == nil {
		return nil, fmt.Errorf("empty meta returned by stor client")
	}

	// it is very first data, save first key to metadata server
	if c.firstMetaKey == nil {
		c.firstMetaKey = key
		if err := c.saveFirstMetaKey(); err != nil {
			return nil, err
		}
	}

	c.lastMd = lastMd
	c.lastMetaKey = key
	c.lastSequence = blocks[len(blocks)-1].Sequence()

	c.storeNum++
	// we don't store last sequence on each iteration because
	// we can easily fetch it using `Walk` method, while it certainly increase
	// processing time.
	// We still store it because otherwise we need to walk the entire disk
	// on startup.
	if c.storeNum%25 == 0 {
		if err := c.saveLastMetaKey(); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// Store stores the val without doing any pre processing
func (c *Client) Store(key, val []byte, prevMd, md *meta.Meta) (*meta.Meta, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	var prevKey []byte

	// assign prevKey if prev metadata is not nil
	if prevMd != nil {
		prevKey = prevMd.Key
	}

	// write to object stor
	md, err := c.storClient.WriteWithMeta(key, val, prevKey, prevMd, md, c.refList)
	if err != nil || prevMd != nil {
		return md, err
	}

	// err == nil && prevMd == nil
	// update first meta key of this vdisk
	c.firstMetaKey = key
	return md, c.saveFirstMetaKey()
}

// SetFirstMetaKey set & store first meta key of this vdisk
func (c *Client) SetFirstMetaKey(key []byte) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.firstMetaKey = key
	return c.saveFirstMetaKey()
}

// SetLastMetaKey set & store last meta key of this vdisk
func (c *Client) SetLastMetaKey(key []byte) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.lastMetaKey = key
	return c.saveLastMetaKey()
}

// PutMeta updates 0-stor metadata
func (c *Client) PutMeta(key []byte, md *meta.Meta) error {
	return c.storClient.PutMeta(key, md)
}

// AppendRefList append reference list to aggregation pointed by the meta
func (c *Client) AppendRefList(md *meta.Meta, refList []string) error {
	return c.storClient.AppendReferenceListWithMeta(md, refList)
}

// Close closes this client
func (c *Client) Close() error {
	c.metaCli.Close()
	return c.storClient.Close()
}

// LastHash return last meta key of this vdisk
func (c *Client) LastHash() []byte {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.lastMetaKey
}

// LoadLastSequence from 0-stor
func (c *Client) LoadLastSequence() (uint64, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	// we don't have flushed block yet
	if c.firstMetaKey == nil {
		return 0, ErrNoFlushedBlock
	}

	if len(c.lastMetaKey) == 0 { // never loaded before
		lastMetaKey, err := c.getLastMetaKey()
		if err != nil {
			return 0, err
		}
		c.lastMetaKey = lastMetaKey
		if c.lastMetaKey == nil {
			c.lastMetaKey = c.firstMetaKey
		}
	}

	// because we don't always write last meta key
	// the last meta key in this stage might be not really the last
	// we get the real last meta now
	lastMeta, err := c.findLastMeta(c.lastMetaKey)
	if err != nil {
		return 0, err
	}
	c.lastMetaKey = lastMeta.Key

	// get last aggregation
	data, _, err := c.storClient.Read(c.lastMetaKey)
	if err != nil {
		return 0, err
	}

	// decode aggregation
	agg, err := c.decodeCapnp(data)
	if err != nil {
		return 0, err
	}

	blocks, err := agg.Blocks()
	if err != nil {
		return 0, err
	}
	if blocks.Len() == 0 {
		return 0, fmt.Errorf("empty blocks for aggregation")
	}

	c.lastSequence = blocks.At(blocks.Len() - 1).Sequence()

	return c.lastSequence, nil
}

// find last metadata of this vdisk
func (c *Client) findLastMeta(startKey []byte) (*meta.Meta, error) {
	key := startKey
	for {
		md, err := c.storClient.GetMeta(key)
		if err != nil {
			return nil, err
		}

		key = md.Next
		if key == nil {
			return md, nil
		}
	}
}

// creates 0-stor client config from Config
func newStorClientConf(conf Config) storclient.Policy {
	return storclient.Policy{
		Organization:           conf.Organization,
		Namespace:              conf.Namespace,
		DataShards:             conf.ZeroStorShards,
		MetaShards:             conf.MetaShards,
		IYOAppID:               conf.IyoClientID,
		IYOSecret:              conf.IyoSecret,
		Compress:               true,
		Encrypt:                true,
		EncryptKey:             conf.EncryptPrivKey,
		ReplicationNr:          0, //force to use distribution
		ReplicationMaxSize:     0, //force to use distribution
		DistributionNr:         conf.DataShardsNum,
		DistributionRedundancy: conf.ParityShardsNum,
	}
}
