package configV2

import (
	"fmt"
	"io/ioutil"

	"github.com/go-yaml/yaml"
)

// YAMLConfig represents a config using yaml as source
type YAMLConfig struct {
	Config *Config
	path   string
}

// NewYAMLConfig returns a new YAMLConfig from provided file path
func NewYAMLConfig(path string) (YAMLConfig, error) {
	var cfg YAMLConfig
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("couldn't create the config: %s", err.Error())
	}
	config, err := fromYAMLBytes(bytes)
	if err != nil {
		return cfg, err
	}

	cfg.Config = config
	cfg.path = path

	if err != nil {
		return cfg, err
	}

	return cfg, nil
}

// creates config from yaml byte slice
func fromYAMLBytes(bytes []byte) (*Config, error) {
	cfg := new(Config)

	// unmarshal the yaml content into our config,
	// which will give us basic validation guarantees
	err := yaml.Unmarshal(bytes, cfg)
	if err != nil {
		return cfg, err
	}

	err = cfg.validate()
	if err != nil {
		return cfg, fmt.Errorf("invalidation configuration: %s", err)
	}

	// semi-specific for the specified user
	if err != nil {
		return cfg, fmt.Errorf("couldn't create the config: %s", err.Error())
	}

	return cfg, nil
}
