package backup

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/zero-os/0-Disk"
	"github.com/zero-os/0-Disk/log"

	valid "github.com/asaskevich/govalidator"
	"github.com/secsy/goftp"
)

// FTPStorageDriverConfig is used to configure and create a FTP (Storage) Driver.
type FTPStorageDriverConfig struct {
	ServerConfig FTPServerConfig
	TLSConfig    *TLSClientConfig
}

// NewFTPServerConfig creates a new FTP Server Config,
// based on a given string
func NewFTPServerConfig(data string) (cfg FTPServerConfig, err error) {
	parts := ftpURLRegexp.FindStringSubmatch(data)
	if len(parts) != 6 {
		err = fmt.Errorf("'%s' is not a valid FTP URL", data)
		return
	}

	// set credentials
	cfg.Username = parts[1]
	cfg.Password = parts[2]

	// set address
	cfg.Address = parts[3] + parts[4]
	if parts[4] == "" {
		cfg.Address += ":21" // port 21 by defualt
	}

	cfg.RootDir = parts[5]

	// all info was set, ensure to validate it while we're at it
	err = cfg.validate()
	return
}

// FTPServerConfig is used to configure and create an FTP (Storage) Driver.
type FTPServerConfig struct {
	// Address of the FTP Server
	Address string

	// Optional: username of Authorized account
	//           for the given FTP server
	Username string
	// Optional: password of Authorized account
	//           for the given FTP server
	Password string

	RootDir string // optional
}

// String implements Stringer.String
func (cfg *FTPServerConfig) String() string {
	if cfg.validate() != nil {
		return "" // invalid config
	}

	address := strings.TrimPrefix(cfg.Address, "ftp://")

	url := "ftp://"
	if cfg.Username != "" {
		url += cfg.Username
		if cfg.Password != "" {
			url += ":" + cfg.Password
		}
		url += "@" + address
	} else {
		url += address
	}

	if cfg.RootDir != "" {
		url += cfg.RootDir
	}

	return url
}

// Set implements flag.Value.Set
func (cfg *FTPServerConfig) Set(str string) error {
	var err error
	*cfg, err = NewFTPServerConfig(str)
	return err
}

// Type implements PFlag.Type
func (cfg *FTPServerConfig) Type() string {
	return "FTPServerConfig"
}

// TLSClientConfig is used to configure client authentication with mutual TLS.
type TLSClientConfig struct {
	Certificate        string
	Key                string
	CA                 string
	ServerName         string
	InsecureSkipVerify bool
}

// tlsClientAuth creates a tls.Config for mutual auth
func tlsClientAuth(cfg TLSClientConfig) (*tls.Config, error) {
	// create client tls config
	tlsCfg := new(tls.Config)

	// load client cert if specified
	if cfg.Certificate != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Certificate, cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("tls client cert: %v", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	tlsCfg.InsecureSkipVerify = cfg.InsecureSkipVerify

	// When no CA certificate is provided, default to the system cert pool
	// that way when a request is made to a server known by the system trust store,
	// the name is still verified
	if cfg.CA != "" {
		// load ca cert
		caCert, err := ioutil.ReadFile(cfg.CA)
		if err != nil {
			return nil, fmt.Errorf("tls client ca: %v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsCfg.RootCAs = caCertPool
	}

	// apply servername overrride
	if cfg.ServerName != "" {
		tlsCfg.InsecureSkipVerify = false
		tlsCfg.ServerName = cfg.ServerName
	}

	tlsCfg.BuildNameToCertificate()
	return tlsCfg, nil
}

// ftpURLRegexp is a simplistic ftp URL regexp,
// to be able to split an FTP url into credentials and the hostname.
// It doesn't contain much validation, as that isn't the point of this regexp.
var ftpURLRegexp = regexp.MustCompile(`^(?:ftp://)?(?:([^:@]+)(?::([^:@]+))?@)?(?:([^@/:]+)(:[0-9]+)?(/.*)?)$`)

// validate the FTP Storage Config.
func (cfg *FTPServerConfig) validate() error {
	if cfg == nil {
		return nil
	}

	if cfg.Address == "" {
		return errors.New("no ftp server address given")
	}
	if !valid.IsDialString(cfg.Address) {
		return fmt.Errorf("invalid ftp server address given: %s", cfg.Address)
	}
	if cfg.RootDir != "" {
		if ok, ft := valid.IsFilePath(cfg.RootDir); !ok || ft != valid.Unix {
			return fmt.Errorf("invalid root dir given: %s", cfg.RootDir)
		}
	}

	if cfg.Password != "" && cfg.Username == "" {
		return errors.New("password given, while username is missing")
	}

	return nil
}

// FTPStorageDriver ceates a driver which allows you
// to read/write deduped blocks/map from/to a FTP server.
func FTPStorageDriver(cfg FTPStorageDriverConfig) (StorageDriver, error) {
	err := cfg.ServerConfig.validate()
	if err != nil {
		return nil, err
	}

	config := goftp.Config{
		User:               cfg.ServerConfig.Username,
		Password:           cfg.ServerConfig.Password,
		ConnectionsPerHost: 10,
		Timeout:            10 * time.Second,
		Logger:             newFTPLogger(cfg.ServerConfig.Address),
	}

	// if a tls config is specified,
	// create an actual TLS Config, and pass it onto the goftp Config
	if cfg.TLSConfig != nil {
		config.TLSConfig, err = tlsClientAuth(*cfg.TLSConfig)
		if err != nil {
			return nil, err
		}
	}

	address := strings.TrimPrefix(cfg.ServerConfig.Address, "ftp://")
	client, err := goftp.DialConfig(config, address)
	if err != nil {
		return nil, err
	}

	return &ftpStorageDriver{
		client:    client,
		knownDirs: newDirCache(),
		rootDir:   strings.Trim(cfg.ServerConfig.RootDir, "/"),
	}, nil
}

type ftpStorageDriver struct {
	client    *goftp.Client
	knownDirs *dirCache
	rootDir   string

	fileExistsMux sync.Mutex
	fileExists    func(string) bool
}

// SetDedupedBlock implements ServerDriver.SetDedupedBlock
func (ftp *ftpStorageDriver) SetDedupedBlock(hash zerodisk.Hash, r io.Reader) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	dir, err := ftp.mkdirs(dir)
	if err != nil {
		return err
	}

	return ftp.lazyStore(path.Join(dir, file), r)
}

// SetHeader implements ServerDriver.SetHeader
func (ftp *ftpStorageDriver) SetHeader(id string, r io.Reader) error {
	dir, err := ftp.mkdirs(backupDir)
	if err != nil {
		return err
	}

	return ftp.lazyStore(path.Join(dir, id), r)
}

// GetDedupedBlock implements ServerDriver.GetDedupedBlock
func (ftp *ftpStorageDriver) GetDedupedBlock(hash zerodisk.Hash, w io.Writer) error {
	dir, file, ok := hashAsDirAndFile(hash)
	if !ok {
		return errInvalidHash
	}

	loc := path.Join(ftp.rootDir, dir, file)
	return ftp.retrieve(loc, w)
}

// GetHeader implements ServerDriver.GetHeader
func (ftp *ftpStorageDriver) GetHeader(id string, w io.Writer) error {
	loc := path.Join(ftp.rootDir, backupDir, id)
	return ftp.retrieve(loc, w)
}

// Close implements ServerDriver.Close
func (ftp *ftpStorageDriver) Close() error {
	return ftp.client.Close()
}

// mkdirs walks through the given directory, one level at a time.
// If a level does not exists yet, it will be created.
// An error is returned if any of the used FTP commands returns an error,
// or in case a file at some point already exist, but is not a directory.
// The returned string is how the client should refer to the created directory.
func (ftp *ftpStorageDriver) mkdirs(dir string) (string, error) {
	dir = path.Join(ftp.rootDir, dir)

	// cheap early check
	// if we already known about the given dir,
	// than we know it exists (or at least we assume it does)
	if ftp.knownDirs.CheckDir(dir) {
		return dir, nil // early success return
	}

	// split the given dir path into directories, level per level
	dirs := strings.Split(dir, "/")

	// start at the driver's root
	var pwd string
	// walk through the entire directory structure, one level at a time
	for _, d := range dirs {
		// join the current level with our last known location
		pwd = path.Join(pwd, d)

		// check if already checked this dir earlier
		if ftp.knownDirs.CheckDir(pwd) {
			continue // current level exists (or we at least assume it does)
		}

		// create the current (sub) directory
		output, err := ftp.client.Mkdir(pwd)
		if err != nil {
			if !isFTPErrorCode(ftpErrorExists, err) {
				return "", err // return unexpected error
			}
		} else {
			pwd = output // only assign output in non-err case
		}

		ftp.knownDirs.AddDir(pwd)
	}

	// all directories either existed or were created
	// return the full path
	return pwd, nil
}

// lazyStore only stores a file if it doesn't exist yet already.
// An error is returned if any of the used FTP commands returns an error,
// or in case the given path already exists and is a directory, instead of a file.
func (ftp *ftpStorageDriver) lazyStore(path string, r io.Reader) error {
	// get info about the given path
	info, err := ftp.client.Stat(path)
	if err != nil {
		// if an error is received,
		// let's check if simply telling us the file doesn't exist yet,
		// if so, let's create it now.
		if isFTPErrorCode(ftpErrorNoExists, err) || isFTPErrorCode(ftpErrorInvalidCommand, err) {
			return ftp.client.Store(path, r)
		}
		return err
	}

	// path existed, and no error was returned.
	// Let's ensure the path points to a file, and not a directory.
	if info.IsDir() {
		return errors.New(path + " is a dir")
	}

	return nil // file already exists, nothing to do
}

// retrieve data from an FTP server.
// returns ErrDataDidNotExist in case there was no data available on the given path.
func (ftp *ftpStorageDriver) retrieve(path string, dest io.Writer) error {
	err := ftp.client.Retrieve(path, dest)
	if isFTPErrorCode(ftpErrorNoExists, err) {
		return ErrDataDidNotExist
	}
	return err
}

// list of ftp error codes we care about
const (
	ftpErrorInvalidCommand = 500
	ftpErrorNoExists       = 550
	ftpErrorExists         = 550
)

// small util function which allows us to check if an error
// is an FTP error, and if so, if it's the code we are looking for.
func isFTPErrorCode(code int, err error) bool {
	if ftpErr, ok := err.(goftp.Error); ok {
		return ftpErr.Code() == code
	}

	return false
}

func newFTPLogger(address string) *ftpLogger {
	return &ftpLogger{
		address: address,
		logger:  log.New("goftp("+address+")", log.GetLevel()),
	}
}

// simple logger used for the FTP driver debug logging
type ftpLogger struct {
	address string
	logger  log.Logger
}

// Write implements io.Writer.Write
func (l ftpLogger) Write(p []byte) (n int, err error) {
	l.logger.Debug(string(p))
	return len(p), nil
}
