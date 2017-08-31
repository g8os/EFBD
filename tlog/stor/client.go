package stor

import (
	"fmt"
	"sync"

	storclient "github.com/zero-os/0-stor/client"
	storclientconf "github.com/zero-os/0-stor/client/config"
	"github.com/zero-os/0-stor/client/lib/compress"
	"github.com/zero-os/0-stor/client/lib/distribution"
	"github.com/zero-os/0-stor/client/lib/encrypt"
	"github.com/zero-os/0-stor/client/lib/hash"
	"github.com/zero-os/0-stor/client/meta"

	"github.com/zero-os/0-Disk/tlog"
	"github.com/zero-os/0-Disk/tlog/schema"
)

const (
	capnpBufLen = 4096 * 4
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

	// etcd key in which we store our last metadata
	// we need to store it so we still know it after restart
	lastMetaEtcdKey  []byte
	firstMetaEtcdKey []byte

	lastMd *meta.Meta

	mux sync.Mutex
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
	}

	firstMetaKey, err := cli.getFirstMetaKey()
	if err != nil {
		return nil, err
	}
	cli.firstMetaKey = firstMetaKey

	lastMetaKey, err := cli.getLastMetaKey()
	if err != nil {
		return nil, err
	}
	cli.lastMetaKey = lastMetaKey

	return cli, nil
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

	key := c.hasher.Hash(data)

	// it is very first data, save first key to metadata server
	if c.firstMetaKey == nil {
		c.firstMetaKey = key
		if err := c.saveFirstMetaKey(); err != nil {
			return nil, err
		}
	}

	// we set our initial meta because
	// we need to set our own timestamp in the 0-stor metadata server
	// in order to have only one epoch for both tlog aggregation and 0-stor metadata
	initialMeta := meta.New(key)
	initialMeta.Epoch = timestamp

	// stor to 0-stor
	lastMd, err := c.storClient.Write(key, data, c.lastMetaKey, c.lastMd, initialMeta)
	if err != nil {
		return nil, err
	}
	if lastMd == nil {
		return nil, fmt.Errorf("empty meta returned by stor client")
	}

	c.lastMd = lastMd
	c.lastMetaKey = key
	return data, c.saveLastMetaKey()
}

func (c *Client) Close() error {
	c.metaCli.Close()
	return c.storClient.Close()
}

func (c *Client) LastHash() []byte {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.lastMetaKey
}

// creates 0-stor client config from Config
func newStorClientConf(conf Config) *storclientconf.Config {
	compressConf := compress.Config{
		Type: compress.TypeSnappy,
	}
	encryptConf := encrypt.Config{
		Type:    encrypt.TypeAESGCM,
		PrivKey: conf.EncryptPrivKey,
	}

	distConf := distribution.Config{
		Data:   conf.DataShardsNum,
		Parity: conf.ParityShardsNum,
	}

	return &storclientconf.Config{
		Organization: conf.Organization,
		Namespace:    conf.Namespace,
		Shards:       conf.ZeroStorShards,
		MetaShards:   conf.MetaShards,
		IYOAppID:     conf.IyoClientID,
		IYOSecret:    conf.IyoSecret,
		Protocol:     "grpc",
		Pipes: []storclientconf.Pipe{
			storclientconf.Pipe{
				Name:   "pipe1",
				Type:   "compress",
				Config: compressConf,
			},
			storclientconf.Pipe{
				Name:   "pipe2",
				Type:   "encrypt",
				Config: encryptConf,
			},

			storclientconf.Pipe{
				Name:   "pipe3",
				Type:   "distribution",
				Config: distConf,
			},
		},
	}
}
