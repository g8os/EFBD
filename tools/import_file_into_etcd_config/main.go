package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/errors"
	yaml "gopkg.in/yaml.v2"
)

func main() {
	// parse flags and positional args
	err := parseFlagsAndArgs()
	exitOnError(err)

	// read file config (source)
	fileConfig, err := readFileConfig()
	exitOnError(err)

	// create etcd config (target)
	importer, err := newImporter()
	exitOnError(err)
	defer importer.Close()

	// write all the StorageClusterConfigs
	for id, storageCluster := range fileConfig.StorageClusters {
		importer.ImportConfig(etcdKey(id, config.KeyClusterStorage), &storageCluster)
	}

	// write all the TlogClusterConfigs
	for id, tlogCluster := range fileConfig.TlogClusters {
		importer.ImportConfig(etcdKey(id, config.KeyClusterTlog), &tlogCluster)
	}

	// write all the VdiskStaticConfigs, VdiskNBDConfigs and VdiskTlogConfigs
	for id, vdiskFullConfig := range fileConfig.Vdisks {
		staticConfig, err := vdiskFullConfig.StaticConfig()
		if !consumeError(err) {
			importer.ImportConfig(etcdKey(id, config.KeyVdiskStatic), staticConfig)

			importer.ImportConfig(etcdKey(id, config.KeyVdiskNBD), vdiskFullConfig.NBD)
			if vdiskFullConfig.Tlog != nil {
				importer.ImportConfig(etcdKey(id, config.KeyVdiskTlog), vdiskFullConfig.Tlog)
			}
		}
	}

	// read and write the NBDVdisksConfig
	nbdVdisksConfig, err := fileConfig.NBDVdisksConfig()
	if !consumeError(err) {
		importer.ImportConfig(etcdKey(cfg.ServerID, config.KeyNBDServerVdisks), nbdVdisksConfig)
	}

	printErrorsAndExit()
}

func etcdKey(id string, keyType config.KeyType) string {
	key, err := config.ETCDKey(id, keyType)
	exitOnError(err)
	return key
}

func readFileConfig() (*config.FileFormatCompleteConfig, error) {
	bytes, err := ioutil.ReadFile(cfg.Path)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't real file config %s", cfg.Path)
	}

	var fileConfig config.FileFormatCompleteConfig
	err = yaml.Unmarshal(bytes, &fileConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't parse file config %s", cfg.Path)
	}

	return &fileConfig, nil
}

func newImporter() (*importer, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		return nil, err
	}
	return &importer{
		client: client,
		ctx:    context.Background(),
	}, nil
}

type importer struct {
	client *clientv3.Client
	ctx    context.Context
}

func (im *importer) ImportConfig(key string, value interface{}) {
	rawValue, err := yaml.Marshal(value)
	if err != nil {
		consumeError(errors.Wrapf(err,
			"couldn't import config %s as value is invalid", key))
		return
	}

	if !cfg.Force {
		resp, err := im.client.Get(im.ctx, key)
		if err == nil && len(resp.Kvs) == 1 {
			if bytes.Compare(resp.Kvs[0].Value, rawValue) == 0 {
				fmt.Fprintf(os.Stderr, "key %s exists and contains the desired value already\r\n", key)
				return
			}
			if !askPermission(key, resp.Kvs[0].Value, rawValue) {
				consumeError(errors.Wrap(err, "user stopped writing config"))
				return
			}
		}
	}

	stringValue := string(rawValue)
	_, err = im.client.Put(im.ctx, key, stringValue)
	if !consumeError(err) {
		fmt.Fprintf(
			os.Stderr, "written config to '%s':\r\n```yaml\r\n%s\r\n```\r\n\r\n",
			key, strings.TrimSpace(stringValue))
	}
}

func (im *importer) Close() {
	im.client.Close()
}

// small util function to ask permission of the user
// to overwrite a config value
// by giving a positive answer on our question.
func askPermission(key string, old, new []byte) bool {
	var response string
	// loop until user responds correctly
	for {
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Their Config:")
		fmt.Fprintln(os.Stderr, string(old))
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Our Config:")
		fmt.Fprintln(os.Stderr, string(new))
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintf(os.Stderr, "overwrite the existing config %v [y/n]: ", key)
		_, err := fmt.Scanln(&response)
		if err != nil {
			continue // no input given, try agian
		}

		// check input
		switch strings.ToLower(response) {
		case "y", "ye", "yes":
			return true
		case "n", "no":
			return false
		default:
			// invalid input, try again
		}
	}
}

func init() {
	// register all optional flags
	flag.BoolVar(&cfg.Force, "f", false,
		"when true, overwrite config values without asking first (default: false)")
	flag.StringVar(&cfg.Path, "path", "config.yml",
		"path to yaml config file (default: config.yml)")
	flag.StringVar(&cfg.ServerID, "id", "default",
		"unique id of the server that will use the etcd cluster")

	// improve usage message
	// (printed in case invalid input was given by the user at startup)
	flag.Usage = func() {
		var exe string
		if len(os.Args) > 0 {
			exe = path.Base(os.Args[0])
		} else {
			exe = "import_file_into_etcd_config"
		}

		fmt.Fprintln(os.Stderr, `
This tool is meant to import a file-based 0-Disk configuration,
into an etcd cluster. This tool is only meant for development and testing purposes,
and should not be used in production.
`)

		fmt.Fprintln(os.Stderr, "Usage:", exe, "[flags] etcd_endpoint...")
		flag.PrintDefaults()
	}
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "CRITICAL ERROR:", err)
		flag.Usage()
		os.Exit(1)
	}
}

func consumeError(err error) bool {
	if err != nil {
		errCollection = append(errCollection, err)
		return true
	}

	return false
}

func printErrorsAndExit() {
	errCollectionCount := len(errCollection)
	if errCollectionCount == 0 {
		fmt.Fprintln(os.Stderr,
			"etcd has all file-originated configs, no errors occured in the process")
		os.Exit(0)
	}

	fmt.Fprintf(
		os.Stderr, "%d error(s) occured while importing %s into etcd\r\n",
		errCollectionCount, cfg.Path)
	for _, err := range errCollection {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(1)
}

var errCollection []error

// parse and apply all flags and positional arguments
func parseFlagsAndArgs() error {
	// parse flags
	flag.Parse()
	if cfg.Path == "" {
		return errors.New("no config path given")
	}
	if cfg.ServerID == "" {
		return errors.New("no server identifier given")
	}

	// parse positional args

	cfg.Endpoints = flag.Args()
	if len(cfg.Endpoints) < 1 {
		return errors.New("at least one etcd endpoint has to be given")
	}

	return nil
}

// The config for this tool,
// filled by the user via flags and positional args.
var cfg struct {
	// Path to the config file
	Path string
	// unique id of the server which will use the etcd cluster,
	// living at the given endpoints
	ServerID string
	// force writing keys,
	// without asking if existing keys should be overwritten
	Force bool
	// Endpoints to the etcd config cluster to be used
	Endpoints []string
}
