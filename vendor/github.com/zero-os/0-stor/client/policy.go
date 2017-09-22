package client

import (
	"fmt"
	"io"
	"io/ioutil"

	validator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

var (
	ErrZeroDataShards              = fmt.Errorf("no data shards specified in the policy")
	ErrZeroMetaShards              = fmt.Errorf("no meta shards specified in the policy")
	ErrNotEnoughReplicationShards  = fmt.Errorf("the number of replication number is bigger then the number of data shards")
	ErrNotEnoughDistributionShards = fmt.Errorf("the number of distribution number is bigger then the number of data shards")
	ErrNoEncryptionKey             = fmt.Errorf("encryption enabled, but encryption key is empty")
)

// Policy repesent the configuration of the client
// It describe how the data will be processed by the client before
// beeing send to the 0-stor servers
type Policy struct {
	// ItsYouOnline organization of the namespace used
	Organization string `yaml:"organization" validate:"nonzero"`
	// Namespace label
	Namespace string `yaml:"namespace" validate:"nonzero"`

	// ItsYouOnline oauth2 application ID
	IYOAppID string `yaml:"iyo_app_id" validate:"nonzero"`
	// ItsYouOnline oauth2 application secret
	IYOSecret string `yaml:"iyo_app_secret" validate:"nonzero"`

	// Addresses to the 0-stor used to store date
	DataShards []string `yaml:"data_shards" validate:"nonzero"`
	// Addresses of the etcd cluster
	MetaShards []string `yaml:"meta_shards" validate:"nonzero"`

	// If the data written to the store is bigger then BlockSize, the data is splitted into
	// blocks of size BlockSize
	// set to 0 to never split data
	BlockSize int `yaml:"block_size"`

	// Number of replication to create when writting
	ReplicationNr int `yaml:"replication_nr"`
	// if data size is smaller than ReplicationMaxSize then data
	// will be replicated ReplicationNr time
	// if data is bigger, distribution will be used if configured
	ReplicationMaxSize int `yaml:"replication_max_size"`

	// Number of data block to create during distribution
	DistributionNr int `yaml:"distribution_data"`
	// Number of parity block to create during distribution
	DistributionRedundancy int `yaml:"distribution_parity"`

	// Enable compression
	Compress bool `yaml:"compress"`
	// Enable ecryption, if true EncryptKey need to be set
	Encrypt bool `yaml:"encrypt"`
	// Key used during encryption
	EncryptKey string `yaml:"encrypt_key"`
}

// ReplicationEnabled return true if replication is set to true and the blockSize is
// smaller then ReplicationMaxSize
func (p Policy) ReplicationEnabled(blockSize int) bool {
	return p.ReplicationMaxSize > 0 && blockSize <= p.ReplicationMaxSize
}

// DistributionEnabled is a helper that check if the distribution is enable in the policy
func (p Policy) DistributionEnabled() bool {
	return p.DistributionNr > 0 && p.DistributionRedundancy > 0
}

// NewPolicyFromReader creates Policy from a reader
func NewPolicyFromReader(r io.Reader) (Policy, error) {
	policy := Policy{}

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return policy, err
	}

	// unmarshall
	if err := yaml.Unmarshal(b, &policy); err != nil {
		return policy, err
	}

	// validate
	if err := policy.validate(); err != nil {
		return policy, err
	}

	return policy, nil
}

// validate make sure that the policy is valid
func (p Policy) validate() error {
	if err := validator.Validate(p); err != nil {
		return err
	}

	if len(p.DataShards) <= 0 {
		return ErrZeroDataShards
	}

	if len(p.MetaShards) <= 0 {
		return ErrZeroMetaShards
	}

	if p.ReplicationNr > 0 && p.ReplicationNr < len(p.DataShards) {
		return ErrNotEnoughReplicationShards
	}

	distributionNr := (p.DistributionNr + p.DistributionRedundancy)
	if distributionNr > 0 && distributionNr < len(p.DataShards) {
		return ErrNotEnoughDistributionShards
	}

	if p.Encrypt && p.EncryptKey == "" {
		return ErrNoEncryptionKey
	}

	return nil
}
