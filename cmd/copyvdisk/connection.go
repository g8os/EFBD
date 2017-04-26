package main

import (
	"github.com/g8os/blockstor/storagecluster"
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

// get 1 or 2 ardb connection(s) based on the given input,
// and the source- and target- (flag) URL Types.
func getARDBConnections(logger log.Logger, input *userInputPair) (connA, connB redis.Conn, err error) {
	var connAString, connBString string

	// get connection strings
	if flagSourceURLType == flagTargetURLType { // source == target && source == grid||direct
		// get source and target
		switch flagSourceURLType {
		case urlTypeGrid:
			connAString, connBString, err = getConnectionStringsFromGrid(logger, input)
		case urlTypeMetaServer:
			connAString, connBString = input.Source.URL, input.Target.URL
		}
	} else if flagSourceURLType == urlTypeGrid { // source = grid && target = direct
		connAString, err = getConnectionStringFromGrid(
			logger, &input.Source, flagSourceStorageCluster)
		connBString = input.Target.URL
	} else { // target = grid && source = direct
		connBString, err = getConnectionStringFromGrid(
			logger, &input.Target, flagTargetStorageCluster)
		connAString = input.Source.URL
	}
	if err != nil {
		return // early return if exit
	}

	// dial first (source) connection string
	connA, err = redis.Dial("tcp", connAString)
	if err != nil {
		return // early return if exit
	}
	if connAString == connBString {
		logger.Info("only 1 TCP connection is required:", connAString)
		return
	}

	// dial second (target) connection string
	connB, err = redis.Dial("tcp", connBString)
	if err != nil {
		connA.Close()
		connA = nil // reset connA again
		return      // early return if exit
	}
	logger.Infof("2 TCP connections required: %q => %q", connAString, connBString)
	return
}

// get 2 meta ardb connection strings from the Grid API
func getConnectionStringsFromGrid(logger log.Logger, input *userInputPair) (connStringA, connStringB string, err error) {
	connStringA, err = getConnectionStringFromGrid(
		logger, &input.Source, flagSourceStorageCluster)
	if err != nil {
		return // early return
	}

	connStringB, err = getConnectionStringFromGrid(
		logger, &input.Target, flagTargetStorageCluster)
	if err != nil {
		connStringA = "" // erase connStringA in case of error
	}

	return
}

// get 1 meta ardb connection string from the Grid API
func getConnectionStringFromGrid(logger log.Logger, input *userInput, scname string) (connString string, err error) {
	cfg, err := storagecluster.NewClusterClient(
		storagecluster.ClusterClientConfig{
			GridAPIAddress:     input.URL,
			VdiskID:            input.VdiskID,
			StorageClusterName: scname,
		},
		logger)
	if err != nil {
		return
	}

	connString, err = cfg.MetaConnectionString()
	return
}
