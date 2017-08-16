package config

import (
	"io"
	"io/ioutil"

	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"

	"github.com/zero-os/0-stor/client/stor"
)

const (
	chunkerStr      = "chunker"
	compressStr     = "compress"
	distributionStr = "distribution"
	encryptStr      = "encrypt"
	hashStr         = "hash"
	replicationStr  = "replication"
	metaStr         = "meta"
	storClientStr   = "stor_client"
)

var (
	validPipes = map[string]struct{}{
		chunkerStr:      struct{}{},
		compressStr:     struct{}{},
		encryptStr:      struct{}{},
		hashStr:         struct{}{},
		replicationStr:  struct{}{},
		distributionStr: struct{}{},
		metaStr:         struct{}{},
		storClientStr:   struct{}{},
	}
)

// Config defines 0-stor client config
type Config struct {
	Organization string `yaml:"organization" validate:"nonzero"`
	Namespace    string `yaml:"namespace" validate:"nonzero"`
	Protocol     string `yaml:"protocol" validate:"nonzero"` // rest or grpc

	IYOAppID  string `yaml:"iyo_app_id"`
	IYOSecret string `yaml:"iyo_app_secret"`

	Shards     []string `yaml:"shards" validate:"nonzero"` // 0-stor shards
	MetaShards []string `yaml:"meta_shards"`
	Pipes      []Pipe   `yaml:"pipes"`
}

// NewFromReader creates Config from a reader
func NewFromReader(r io.Reader) (*Config, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var conf Config

	// unmarshall
	if err := yaml.Unmarshal(b, &conf); err != nil {
		return nil, err
	}

	// validate
	if err := validator.Validate(conf); err != nil {
		return nil, err
	}

	conf.CheckAppendStorClient()

	// do post processing to each pipe
	for i, pipe := range conf.Pipes {
		if err := pipe.validate(); err != nil {
			return nil, err
		}

		if err := pipe.setConfigType(); err != nil {
			return nil, err
		}
		conf.Pipes[i] = pipe
	}
	return &conf, nil
}

// Write writes the config to the given writer
func (conf *Config) Write(w io.Writer) error {
	b, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}

	_, err = w.Write(b)
	return err
}

func (conf *Config) ChunkerFirstPipe() bool {
	if len(conf.Pipes) == 0 {
		return false
	}
	return conf.Pipes[0].Type == chunkerStr
}

// CheckAppendStorClient checks if we need to add stor.Client in the pipe
// append it if needed
func (conf *Config) CheckAppendStorClient() {
	if !conf.needStorClientInPipe() {
		return
	}
	pipe := Pipe{
		Type:   storClientStr,
		Config: stor.Config{},
	}
	conf.Pipes = append(conf.Pipes, pipe)
}

// check if this config need stor.Client to be added
// in the end of pipe
// - pipe is empty
// - no distribution, storClient, or replication specified
func (conf *Config) needStorClientInPipe() bool {
	if len(conf.Pipes) == 0 {
		return true
	}

	for _, pipe := range conf.Pipes {
		if pipe.Type == storClientStr || pipe.Type == distributionStr || pipe.Type == replicationStr {
			return false
		}
	}
	return true
}
