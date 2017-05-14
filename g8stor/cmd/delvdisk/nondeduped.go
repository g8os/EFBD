package delvdisk

import (
	"fmt"

	"github.com/g8os/blockstor/g8stor/cmd/config"
	"github.com/g8os/blockstor/log"
	"github.com/garyburd/redigo/redis"
	"github.com/spf13/cobra"
)

// NondedupedCmd represents the nondeduped delete subcommand
var NondedupedCmd = &cobra.Command{
	Use:   "nondeduped vdiskid ardb_url",
	Short: "Delete the data (blocks) of a nondeduped vdisk",
	RunE:  deleteNondeduped,
}

func deleteNondeduped(cmd *cobra.Command, args []string) error {
	// create logger
	var logger log.Logger
	if config.Verbose {
		// log info to stderr
		logger = log.New("delete-nondeduped", log.InfoLevel)
	} else {
		// discard all logs
		logger = log.NopLogger()
	}

	// parse user input
	logger.Info("parsing positional arguments...")
	input, err := parseUserInput(args)
	if err != nil {
		return err
	}

	// get ardb connections
	logger.Info("get the redis connection...")
	conn, err := redis.Dial("tcp", input.URL)
	if err != nil {
		return err
	}
	defer conn.Close()

	// ensure vdisk exists
	if exists, _ := redis.Bool(conn.Do("EXISTS", input.VdiskID)); !exists {
		return fmt.Errorf("vdisk %q does not exist", input.VdiskID)
	}

	// delete nondeduped data
	logger.Infof("deleting vdisk %q...", input.VdiskID)
	_, err = conn.Do("DEL", input.VdiskID)
	return err
}
