package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func main() {
	// parse user input
	input, err := parseUserInput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid user input: %s\n", err.Error())
		printUsage()
		os.Exit(2)
	}

	// create logger
	var logger log.Logger
	if flagVerbose {
		// log info to stderr
		logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)
	} else {
		// discard all logs
		logger = log.New(ioutil.Discard, "", 0)
	}

	logger.Info("get the redis connection(s)...")

	var connA, connB redis.Conn
	switch flagURLType {
	case urlTypeGrid:
		connA, connB, err = getConnectionsFromGrid(logger, input)
	case urlTypeMetaServer:
		connA, connB, err = getConnectionsFromMetaServer(logger, input)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"couldn't get the redis connection(s): %s\n", err.Error())
		printUsage()
		os.Exit(1)
	}

	logger.Infof("copy volume %q as %q",
		input.Source.Volume, input.Target.Volume)

	if connB == nil {
		err = copySameConnection(logger, input, connA)
	} else {
		err = copyDifferentConnections(logger, input, connA, connB)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"couldn't copy volume %q as %q: %s\n",
			input.Source.Volume, input.Target.Volume, err.Error())
		printUsage()
		os.Exit(1)
	}

	logger.Infof("copied succesfully volume %q to volume %q",
		input.Source.Volume, input.Target.Volume)
}

func init() {
	// register flags
	flag.Var(&flagURLType, "t", "type of the given url(s); the gridapi url's or the direct metadataserver connectionstrings")
	flag.BoolVar(&flagVerbose, "v", false, "log to stderr")
	flag.StringVar(&flagSourceStorageCluster, "sourcesc", "",
		"combined with api type it allows you to predefine the source's storageCluster name")
	flag.StringVar(&flagTargetStorageCluster, "targetsc", "",
		"combined with api type it allows you to predefine the target's storageCluster name")

	// custom usage function
	flag.Usage = printUsage

	// parse flags
	flag.Parse()
}

// parse user input from the positional arguments
// given by the user when calling this command line utility
func parseUserInput() (input *userInput, err error) {
	args := flag.Args()
	argn := len(args)

	if argn < 3 {
		err = errors.New("not enough arguments")
		return
	}
	if argn > 4 {
		err = errors.New("too many arguments")
		return
	}

	input = new(userInput)

	// store required args
	input.Source.Volume = args[0]
	input.Target.Volume = args[1]
	input.Source.URL = args[2]

	// store optional args
	if argn == 4 {
		input.Target.URL = args[3]
	} else {
		// if no explicit target_url is given
		// it is assumed that the same url
		// for both source and target is to be used
		input.Target.URL = args[2]
	}

	return
}

// print the name, version, short description
// and usage strings for this command line utility
func printUsage() {
	exe := path.Base(os.Args[0])
	fmt.Fprintf(os.Stderr, usage, exe, urlTypeGrid, urlTypeMetaServer)
}

// userInput is parsed from the positional arguments
type userInput struct {
	Source struct {
		Volume string
		URL    string
	}
	Target struct {
		Volume string
		URL    string
	}
}

// optional flags
var (
	flagURLType              = urlType(urlTypeGrid)
	flagVerbose              bool
	flagTargetStorageCluster string
	flagSourceStorageCluster string
)

// usage string
const (
	usage = `copyvolume 1.1.0

copy the metadata of a deduped volume

usage:
  %[1]s [-v] [-t %[2]s|%[3]s] [-sourcesc name] [-targetsc name] source_volume target_volume source_url [target_url]

  When no target_url is given, the target_url is the same as the source_url.
`
)

type urlType string

// url types
const (
	urlTypeGrid       = "api"
	urlTypeMetaServer = "direct"
)

// String implements flag.Value.String
func (t *urlType) String() string {
	return string(*t)
}

// Set implements flag.Value.Set
func (t *urlType) Set(raw string) (err error) {
	switch strings.ToLower(raw) {
	case urlTypeGrid, urlTypeMetaServer:
		*t = urlType(raw)
	default:
		err = fmt.Errorf("%q is not a valid url type", raw)
	}

	return
}
