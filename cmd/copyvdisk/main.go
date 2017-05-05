package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"

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
	flagVerboseTag = "v"
)

func init() {
	// register flags
	flag.BoolVar(&flagVerbose, flagVerboseTag, false, "")

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
	}

	return
}

// print the name, version, short description
// and usage strings for this command line utility
func printUsage() {
	exe := path.Base(os.Args[0])
	fmt.Fprintf(os.Stderr,
		usage, exe,
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
	flagVerbose bool
)

// usage string
const (
	usage = `copyvdisk 1.1.0

copy the metadata of a deduped vdisk

usage:
  %[1]s [-%[2]s] \
    source_vdisk target_vdisk source_url [target_url]

  -%[2]s:
    log all available info to the STDERR

  -h, --help:
    print this usage message

  When no target_url is given,
  the target_url is the same as the source_url
`
)
