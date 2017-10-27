package config

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/log"
	yaml "gopkg.in/yaml.v2"
)

// FileSource creates a config source,
// where the configurations originate from a file on the local file system.
// WARNING: this is only to be used for development and testing purposes,
// it is by no means intended for production.
func FileSource(path string) (SourceCloser, error) {
	return &fileSource{
		path:   path,
		reader: ioutil.ReadFile,
	}, nil
}

type fileSource struct {
	path   string
	reader func(string) ([]byte, error)
}

// Get implements Source.Get
func (s *fileSource) Get(key Key) ([]byte, error) {
	switch key.Type {
	case KeyVdiskStatic:
		vdiskConfig, err := s.readVdiskConfig(key.ID)
		if err != nil {
			return nil, err
		}
		return serializeConfigReply(vdiskConfig.StaticConfig())

	case KeyVdiskNBD:
		vdiskConfig, err := s.readVdiskConfig(key.ID)
		if err != nil {
			return nil, err
		}
		return serializeConfigReply(vdiskConfig.NBDConfig())

	case KeyVdiskTlog:
		vdiskConfig, err := s.readVdiskConfig(key.ID)
		if err != nil {
			return nil, err
		}
		return serializeConfigReply(vdiskConfig.TlogConfig())

	case KeyClusterStorage:
		return serializeConfigReply(s.readStorageClusterConfig(key.ID))

	case KeyClusterZeroStor:
		return serializeConfigReply(s.readZeroStorClusterConfig(key.ID))

	case KeyClusterTlog:
		return serializeConfigReply(s.readTlogClusterConfig(key.ID))

	case KeyNBDServerVdisks:
		// for file config we ignore the key ID here,
		// as we don't support multipe nbdservers with one config file.
		return serializeConfigReply(s.readNBDVdisksConfig())

	default:
		return nil, errors.Wrapf(
			ErrInvalidKey,
			"%v is not a supported key type by the file config",
			key.Type,
		)
	}
}

// Watch implements Source.Watch
func (s *fileSource) Watch(ctx context.Context, key Key) (<-chan []byte, error) {
	// setup SIGHUP
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)

	ch := make(chan []byte)

	go func() {
		log.Debugf("Started (SIGHUP) watch goroutine for: %v", key)
		defer signal.Stop(sighup)
		defer close(ch)
		defer log.Debugf("Closing (SIGHUP) watch goroutine for %v", key)

		for {
			select {
			case <-ctx.Done():
				return
			case <-sighup:
				log.Debug("Received SIGHUP for: ", s.path)
				// read, deserialize and serialize sub config
				output, err := s.Get(key)
				if errors.Cause(err) == ErrSourceUnavailable {
					log.Errorf(
						"getting key %v failed, due to the source being unavailable",
						key)
					return
				}

				select {
				case ch <- output:
				case <-ctx.Done():
					log.Errorf(
						"timed out while attempting to send updated config (%d)", key.Type)
				}
			}
		}
	}()

	return ch, nil
}

// MarkInvalidKey implements Source.MarkInvalidKey
func (s *fileSource) MarkInvalidKey(key Key, vdiskID string) {
	if vdiskID == "" {
		log.Errorf("%v in '%s' is invalid", key, s.path)
		return
	}

	log.Errorf("%v in '%s' is invalid when used for vdisk %s",
		key, s.path, vdiskID)
}

// SourceConfig implements Source.SourceConfig
func (s *fileSource) SourceConfig() interface{} {
	return s.path
}

// Type implements Source.Type
func (s *fileSource) Type() string {
	return "file"
}

// Close implements Source.Close
func (s *fileSource) Close() error {
	return nil
}

// read the entire config from file,
// and take out a specific vdisk config
func (s *fileSource) readVdiskConfig(vdiskID string) (*FileFormatVdiskConfig, error) {
	cfg, err := s.readFullFile()
	if err != nil {
		return nil, err
	}

	return cfg.VdiskConfig(vdiskID)
}

// read the entire config from file,
// and take out the NBD vdisks config.
func (s *fileSource) readNBDVdisksConfig() (*NBDVdisksConfig, error) {
	cfg, err := s.readFullFile()
	if err != nil {
		return nil, err
	}

	return cfg.NBDVdisksConfig()
}

// read the entire config from file,
// and take out a specific storage cluster config
func (s *fileSource) readStorageClusterConfig(clusterID string) (*StorageClusterConfig, error) {
	cfg, err := s.readFullFile()
	if err != nil {
		return nil, err
	}

	return cfg.StorageClusterConfig(clusterID)
}

func (s *fileSource) readZeroStorClusterConfig(clusterID string) (*ZeroStorClusterConfig, error) {
	cfg, err := s.readFullFile()
	if err != nil {
		return nil, err
	}

	return cfg.ZeroStorClusterConfig(clusterID)
}

// read the entire config from file,
// and take out a specific tlog cluster config
func (s *fileSource) readTlogClusterConfig(clusterID string) (*TlogClusterConfig, error) {
	cfg, err := s.readFullFile()
	if err != nil {
		return nil, err
	}

	return cfg.TlogClusterConfig(clusterID)
}

// read the entire config from file
func (s *fileSource) readFullFile() (*FileFormatCompleteConfig, error) {
	bytes, err := s.reader(s.path)
	if err != nil {
		log.Errorf("couldn't read file config: %v", err)
		return nil, ErrSourceUnavailable
	}

	var cfg FileFormatCompleteConfig
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		log.Errorf("invalid file config: %v", err)
		return nil, NewInvalidConfigError(err)
	}

	return &cfg, nil
}

// FileFormatCompleteConfig is the YAML format struct
// used for a zerodisk config file.
type FileFormatCompleteConfig struct {
	Vdisks           map[string]FileFormatVdiskConfig `yaml:"vdisks" valid:"required"`
	StorageClusters  map[string]StorageClusterConfig  `yaml:"storageClusters" valid:"required"`
	TlogClusters     map[string]TlogClusterConfig     `yaml:"tlogClusters" valid:"optional"`
	ZeroStorClusters map[string]ZeroStorClusterConfig `yaml:"zeroStorClusters" valid:"optional"`
}

// NBDVdisksConfig returns the NBD Vdisks configuration embedded in
// the YAML zerodisk config file.
func (cfg *FileFormatCompleteConfig) NBDVdisksConfig() (*NBDVdisksConfig, error) {
	nbdVdisksConfig := new(NBDVdisksConfig)
	for vdiskID := range cfg.Vdisks {
		nbdVdisksConfig.Vdisks = append(nbdVdisksConfig.Vdisks, vdiskID)
	}
	return nbdVdisksConfig, nil
}

// VdiskConfig returns the full vdisk configuration embedded in
// the YAML zerodisk config file.
func (cfg *FileFormatCompleteConfig) VdiskConfig(id string) (*FileFormatVdiskConfig, error) {
	vdiskConfig, ok := cfg.Vdisks[id]
	if !ok {
		return nil, NewInvalidConfigError(
			errors.New("file config has no vdisk config under the id " + id),
		)
	}
	return &vdiskConfig, nil
}

// StorageClusterConfig returns the StorageCluster configuration embedded in
// the YAML zerodisk config file.
func (cfg *FileFormatCompleteConfig) StorageClusterConfig(id string) (*StorageClusterConfig, error) {
	storageClusterConfig, ok := cfg.StorageClusters[id]
	if !ok {
		log.Debug("file config has no storage cluster config under the id ", id)
		return nil, ErrConfigUnavailable
	}
	return &storageClusterConfig, nil
}

// ZeroStorClusterConfig returns ZeroStorCluster configuration embedded in
// the YAML zerodisk config file
func (cfg *FileFormatCompleteConfig) ZeroStorClusterConfig(id string) (*ZeroStorClusterConfig, error) {
	zeroStorClusterConfig, ok := cfg.ZeroStorClusters[id]
	if !ok {
		log.Debug("file config has no 0-stor cluster config under the id ", id)
		return nil, ErrConfigUnavailable
	}
	return &zeroStorClusterConfig, nil
}

// TlogClusterConfig returns the TlogCluster configuration embedded in
// the YAML zerodisk config file.
func (cfg *FileFormatCompleteConfig) TlogClusterConfig(id string) (*TlogClusterConfig, error) {
	tlogClusterConfig, ok := cfg.TlogClusters[id]
	if !ok {
		return nil, NewInvalidConfigError(
			errors.New("file config has no tlog cluster config under the id " + id),
		)
	}
	return &tlogClusterConfig, nil
}

// FileFormatVdiskConfig is the YAML format struct
// used for all vdisk file-originated configurations.
type FileFormatVdiskConfig struct {
	BlockSize       uint64    `yaml:"blockSize" valid:"required"`
	ReadOnly        bool      `yaml:"readOnly" valid:"optional"`
	Size            uint64    `yaml:"size" valid:"required"`
	VdiskType       VdiskType `yaml:"type" valid:"required"`
	TemplateVdiskID string    `yaml:"vdiskTemplateID" valid:"required"`

	NBD  *VdiskNBDConfig  `yaml:"nbd" valid:"optional"`
	Tlog *VdiskTlogConfig `yaml:"tlog" valid:"optional"`
}

// StaticConfig returns the vdisk's Static configuration embedded in
// the vdisk config file format.
func (cfg *FileFormatVdiskConfig) StaticConfig() (*VdiskStaticConfig, error) {
	static := &VdiskStaticConfig{
		BlockSize:       cfg.BlockSize,
		ReadOnly:        cfg.ReadOnly,
		Size:            cfg.Size,
		Type:            cfg.VdiskType,
		TemplateVdiskID: cfg.TemplateVdiskID,
	}

	return static, nil
}

// NBDConfig returns the vdisk's NBD configuration embedded in
// the vdisk config file format.
func (cfg *FileFormatVdiskConfig) NBDConfig() (*VdiskNBDConfig, error) {
	if cfg.NBD == nil {
		return nil, NewInvalidConfigError(errors.New("vdisk has no NBD configuration"))
	}
	return cfg.NBD, nil
}

// TlogConfig returns the vdisk's Tlog configuration embedded in
// the vdisk config file format.
func (cfg *FileFormatVdiskConfig) TlogConfig() (*VdiskTlogConfig, error) {
	if cfg.Tlog == nil {
		return nil, NewInvalidConfigError(errors.New("vdisk has no Tlog configuration"))
	}
	return cfg.Tlog, nil
}

// if no error is given, we serialize the given value (unless it's nil)
// into the YAML format, and return it (or an error if that didn't go well either).
func serializeConfigReply(value interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("nil value can't be serialized")
	}

	return yaml.Marshal(value)
}
