package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	log "github.com/glendc/go-mini-log"
)

func main() {
	// create logger
	var logger log.Logger
	if flagVerbose {
		// log info to stderr
		logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)
	} else {
		// discard all logs
		logger = log.New(ioutil.Discard, "", 0)
	}

	// parse user input
	input, err := parseUserInput(logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid user input: %s\n", err.Error())
		printUsage()
		os.Exit(2)
	}

	// user feedback based on wrong flag pairing
	if flagSourceURLType == urlTypeMetaServer && flagSourceStorageCluster != "" {
		// defining storage cluster name is useless when the url type is direct
		logger.Infof(
			"-%s is defined as %q but will be ignored as -%s is of type %q",
			flagSourceStorageClusterTag, flagSourceStorageCluster,
			flagSourceURLTypeTag, flagSourceURLType)
	}
	if flagTargetURLType == urlTypeMetaServer && flagTargetStorageCluster != "" {
		// defining storage cluster name is useless when the url type is direct
		logger.Infof(
			"-%s is defined as %q but will be ignored as -%s is of type %q",
			flagTargetStorageClusterTag, flagTargetStorageCluster,
			flagTargetURLTypeTag, flagTargetURLType)
	}

	logger.Info("get the redis connection(s)...")

	connA, connB, err := getARDBConnections(logger, input)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"couldn't get the redis connection(s): %s\n", err.Error())
		printUsage()
		os.Exit(1)
	}

	logger.Infof("copy vdisk %q as %q",
		input.Source.VdiskID, input.Target.VdiskID)

	if connB == nil {
		err = copySameConnection(logger, input, connA)
	} else {
		err = copyDifferentConnections(logger, input, connA, connB)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"couldn't copy vdisk %q as %q: %s\n",
			input.Source.VdiskID, input.Target.VdiskID, err.Error())
		printUsage()
		os.Exit(1)
	}

	logger.Infof("copied succesfully vdisk %q to vdisk %q",
		input.Source.VdiskID, input.Target.VdiskID)
}

const (
	flagSourceURLTypeTag        = "sourcetype"
	flagTargetURLTypeTag        = "targettype"
	flagVerboseTag              = "v"
	flagSourceStorageClusterTag = "sourcesc"
	flagTargetStorageClusterTag = "targetsc"
)

func init() {
	// register flags
	flag.BoolVar(&flagVerbose, flagVerboseTag, false, "")
	flag.Var(&flagSourceURLType, flagSourceURLTypeTag, "")
	flag.Var(&flagTargetURLType, flagTargetURLTypeTag, "")
	flag.StringVar(&flagSourceStorageCluster, flagSourceStorageClusterTag, "", "")
	flag.StringVar(&flagTargetStorageCluster, flagTargetStorageClusterTag, "", "")

	// custom usage function
	flag.Usage = printUsage

	// parse flags
	flag.Parse()
}

// parse user input from the positional arguments
// given by the user when calling this command line utility
func parseUserInput(logger log.Logger) (input *userInputPair, err error) {
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

	input = new(userInputPair)

	// store required args
	input.Source.VdiskID = args[0]
	input.Target.VdiskID = args[1]
	input.Source.URL = args[2]

	// store optional args
	if argn == 4 {
		input.Target.URL = args[3]
	} else {
		// if no explicit target_url is given
		// it is assumed that the same url
		// for both source and target is to be used
		input.Target.URL = args[2]
		// in which case we'll also assume -targettype == -soucetype
		if flagTargetURLType != flagSourceURLType {
			logger.Infof(
				"-%s is defined as %q but will be defaulted to %q (-%s), as only 1 url is specified",
				flagTargetURLTypeTag, flagTargetURLType, flagSourceURLType, flagSourceURLTypeTag)
		}
		flagTargetURLType = flagSourceURLType
	}

	return
}

// print the name, version, short description
// and usage strings for this command line utility
func printUsage() {
	exe := path.Base(os.Args[0])
	fmt.Fprintf(os.Stderr,
		usage, exe,
		flagSourceURLTypeTag, flagTargetURLTypeTag, urlTypeGrid, urlTypeMetaServer,
		flagSourceStorageClusterTag, flagTargetStorageClusterTag,
		flagVerboseTag)
}

type userInput struct {
	VdiskID string
	URL     string
}

// userInputPair is parsed from the positional arguments
type userInputPair struct {
	Source userInput
	Target userInput
}

// optional flags
var (
	flagSourceURLType        = urlType(urlTypeGrid)
	flagTargetURLType        = urlType(urlTypeGrid)
	flagVerbose              bool
	flagTargetStorageCluster string
	flagSourceStorageCluster string
)

// usage string
const (
	usage = `copyvdisk 1.1.0

copy the metadata of a deduped vdisk

usage:
  %[1]s [-%[8]s] \
    [-%[2]s %[4]s|%[5]s] [-%[3]s %[4]s|%[5]s] \
    [-%[6]s name] [-%[7]s name] \
    source_vdisk target_vdisk source_url [target_url]

  -%[2]s, -%[3]s:
    types of the given url(s), default="%[4]s", options:
      => "%[4]s": specify an url to use the GridAPI for the source/target;
      => "%[5]s": specify a connection string to use an ARDB directly;

  -%[6]s, -%[7]s:
    combined with the "%[2]s and/or %[3]s" flag(s),
    it allows you to predefine the source's and/or target's storageCluster name,
    instead of fetching it automatically from the GridAPI's vdisk info

  -%[8]s:
    log all available info to the STDERR

  -h, --help:
    print this usage message

  When no target_url is given,
  the target_url is the same as the source_url
  and the type of target_url (-%[3]s)
  will be the same as the type of source_url (-%[2]s).
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
