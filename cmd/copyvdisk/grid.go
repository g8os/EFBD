package main

import (
	"github.com/g8os/blockstor/storagecluster"
	"github.com/garyburd/redigo/redis"
	log "github.com/glendc/go-mini-log"
)

func getConnectionsFromGrid(logger log.Logger, input *userInput) (connA, connB redis.Conn, err error) {
	cfgA, err := storagecluster.NewClusterConfig(
		input.Source.URL, input.Source.VdiskID, flagSourceStorageCluster, logger)
	if err != nil {
		return
	}
	urlA, err := cfgA.MetaConnectionString()
	if err != nil {
		return
	}
	connA, err = redis.Dial("tcp", urlA)
	if err != nil {
		return
	}

	cfgB, err := storagecluster.NewClusterConfig(
		input.Target.URL, input.Target.VdiskID, flagTargetStorageCluster, logger)
	if err != nil {
		return
	}
	urlB, err := cfgB.MetaConnectionString()
	if err != nil {
		return
	}
	if urlB == urlA {
		logger.Info("only 1 TCP connection is required:", urlA)
		return // only 1 connection needed
	}

	logger.Infof("2 TCP connections required: %q => %q", urlA, urlB)
	connB, err = redis.Dial("tcp", urlB)
	return
}
