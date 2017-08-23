package config

import (
	"fmt"
	"io"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/lib/chunker"
	"github.com/zero-os/0-stor/client/lib/compress"
	"github.com/zero-os/0-stor/client/lib/distribution"
	"github.com/zero-os/0-stor/client/lib/encrypt"
	"github.com/zero-os/0-stor/client/lib/hash"
	"github.com/zero-os/0-stor/client/lib/replication"
	mb "github.com/zero-os/0-stor/client/meta/block"
	"github.com/zero-os/0-stor/client/stor"
)

// Pipe defines each 0-stor client pipe
type Pipe struct {
	Name string `yaml:"name"`

	// Type of this pipe, must be one of:
	// chunker, compress, distribution, encrypt, hash, replication
	Type string `yaml:"type" validate:"nonzero"`

	Config interface{} `yaml:"config"`
}

// CreateBlockReader creates a block reader
func (p Pipe) CreateBlockReader(shards, metaShards []string, proto, org, namespace,
	iyoAppID, iyoAppSecret string) (block.Reader, error) {

	switch p.Type {
	case compressStr:
		conf := p.Config.(compress.Config)
		return compress.NewReader(conf)
	case encryptStr:
		conf := p.Config.(encrypt.Config)
		return encrypt.NewReader(conf)
	case distributionStr:
		conf := p.Config.(distribution.Config)
		return distribution.NewStorRestorer(conf, shards, metaShards, proto, org, namespace, iyoAppID, iyoAppSecret)
	case replicationStr:
		conf := p.Config.(replication.Config)
		return replication.NewStorReader(conf, shards, metaShards, org, namespace, iyoAppID, iyoAppSecret, proto)
	case metaStr:
		conf := p.Config.(mb.Config)
		return mb.NewReader(conf, metaShards)
	case chunkerStr:
		conf := p.Config.(chunker.Config)
		return chunker.NewBlockReader(conf, metaShards)
	case storClientStr:
		conf := stor.Config{
			Protocol:    proto,
			IyoClientID: iyoAppID,
			IyoSecret:   iyoAppSecret,
		}
		return stor.NewReader(conf, shards, metaShards, org, namespace)
	default:
		return nil, fmt.Errorf("invalid reader type:%v", p.Type)
	}
}

// CreateBlockWriter creates block writer
func (p Pipe) CreateBlockWriter(w block.Writer, shards, metaShards []string, proto, org, namespace,
	iyoAppID, iyoAppSecret string, r io.Reader) (block.Writer, error) {
	switch p.Type {
	case compressStr:
		conf := p.Config.(compress.Config)
		return compress.NewWriter(conf, w)
	case distributionStr:
		conf := p.Config.(distribution.Config)
		return distribution.NewStorDistributor(w, conf, shards, metaShards, proto, org, namespace, iyoAppID, iyoAppSecret)
	case replicationStr:
		conf := p.Config.(replication.Config)
		return replication.NewStorWriter(w, conf, shards, metaShards, org, namespace, iyoAppID, iyoAppSecret, proto)
	case encryptStr:
		conf := p.Config.(encrypt.Config)
		return encrypt.NewWriter(w, conf)
	case hashStr:
		conf := p.Config.(hash.Config)
		return hash.NewWriter(w, conf)
	case metaStr:
		conf := p.Config.(mb.Config)
		return mb.NewWriter(w, conf, metaShards)
	case chunkerStr:
		conf := p.Config.(chunker.Config)
		return chunker.NewBlockWriter(w, conf, metaShards, r)
	case storClientStr:
		conf := stor.Config{
			Protocol:    proto,
			IyoClientID: iyoAppID,
			IyoSecret:   iyoAppSecret,
		}
		return stor.NewWriter(w, conf, shards, metaShards, org, namespace)
	default:
		return nil, fmt.Errorf("invalid writer type:%v", p.Type)
	}
}

// set p.Config to proper type.
// because by default the parser is going to
// set tyhe type as map[string]interface{}
func (p *Pipe) setConfigType() error {
	b, err := yaml.Marshal(p.Config)
	if err != nil {
		return err
	}

	switch p.Type {
	case chunkerStr:
		var conf chunker.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case compressStr:
		var conf compress.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case encryptStr:
		var conf encrypt.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case hashStr:
		var conf hash.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case replicationStr:
		var conf replication.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case distributionStr:
		var conf distribution.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case metaStr:
		var conf mb.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	case storClientStr:
		var conf stor.Config
		if err := yaml.Unmarshal(b, &conf); err != nil {
			return err
		}
		p.Config = conf

	default:
		panic("invalid pipe type:" + p.Type)

	}
	return nil
}

// validate a pipe config
func (p Pipe) validate() error {
	if err := validator.Validate(p); err != nil {
		return err
	}

	if _, ok := validPipes[p.Type]; !ok {
		return fmt.Errorf("invalid pipe: %v", p.Type)
	}

	return nil
}
