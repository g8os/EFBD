package copyvdisk

import (
	"fmt"
	"strconv"

	"github.com/g8os/blockstor/g8stor/cmd/config"
	"github.com/g8os/blockstor/log"
	"github.com/garyburd/redigo/redis"
	"github.com/spf13/cobra"
)

// DedupedCmd represents the deduped copy subcommand
var DedupedCmd = &cobra.Command{
	Use:   "deduped source_vdisk target_vdisk source_url [target_url]",
	Short: "Copy the metadata of a deduped vdisk",
	RunE:  copyDeduped,
}

func copyDeduped(cmd *cobra.Command, args []string) error {
	// create logger
	var logger log.Logger
	if config.Verbose {
		// log info to stderr
		logger = log.New("copy-deduped", log.InfoLevel)
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
	logger.Info("get the redis connection(s)...")
	connA, connB, err := getARDBConnections(input, logger)
	if err != nil {
		return err
	}

	logger.Infof("copy vdisk %q as %q",
		input.Source.VdiskID, input.Target.VdiskID)

	if connB == nil {
		err = copyDedupedSameConnection(input, connA, logger)
	} else {
		err = copyDedupedDifferentConnections(input, connA, connB, logger)
	}
	if err != nil {
		return err
	}

	logger.Infof("copied succesfully vdisk %q to vdisk %q",
		input.Source.VdiskID, input.Target.VdiskID)
	return nil
}

func copyDedupedSameConnection(input *userInputPair, conn redis.Conn, logger log.Logger) (err error) {
	defer conn.Close()

	script := redis.NewScript(0, `
if #ARGV ~= 2 then
    local usage = "copyDedupedSameConnection script usage: source destination"
    redis.log(redis.LOG_NOTICE, usage)
    error("copyDedupedSameConnection script requires 2 arguments (source destination)")
end

local source = ARGV[1]
local destination = ARGV[2]

if redis.call("EXISTS", source) == 0 then
    return redis.error_reply('"' .. source .. '" does not exist')
end

if redis.call("EXISTS", destination) == 1 then
    redis.call("DEL", destination)
end

redis.call("RESTORE", destination, 0, redis.call("DUMP", source))

return redis.call("HLEN", destination)
`)

	logger.Infof("dumping vdisk %q and restoring it as vdisk %q",
		input.Source.VdiskID, input.Target.VdiskID)

	indexCount, err := redis.Int64(script.Do(conn,
		input.Source.VdiskID, input.Target.VdiskID))
	if err == nil {
		logger.Infof("copied %d meta indices to vdisk %q",
			indexCount, input.Target.VdiskID)
	}

	return
}

func copyDedupedDifferentConnections(input *userInputPair, connA, connB redis.Conn, logger log.Logger) (err error) {
	defer func() {
		connA.Close()
		connB.Close()
	}()

	// get data from source connection
	logger.Infof("collecting all metadata from source vdisk %q...", input.Source.VdiskID)
	data, err := redis.StringMap(connA.Do("HGETALL", input.Source.VdiskID))
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = fmt.Errorf("%q does not exist", input.Source.VdiskID)
		return
	}
	logger.Infof("collected %d meta indices from source vdisk %q",
		len(data), input.Source.VdiskID)

	// ensure the vdisk isn't touched while we're creating it
	if err = connB.Send("WATCH", input.Target.VdiskID); err != nil {
		return
	}

	// start the copy transaction
	if err = connB.Send("MULTI"); err != nil {
		return
	}

	// delete any existing vdisk
	if err = connB.Send("DEL", input.Target.VdiskID); err != nil {
		return
	}

	// buffer all data on target connection
	logger.Infof("buffering %d meta indices for target vdisk %q...",
		len(data), input.Target.VdiskID)
	var index int64
	for rawIndex, hash := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", input.Target.VdiskID, index, []byte(hash))
	}

	// send all data to target connection (execute the transaction)
	logger.Infof("flushing buffered metadata for target vdisk %q...", input.Target.VdiskID)
	response, err := connB.Do("EXEC")
	if err == nil && response == nil {
		// if response == <nil> the transaction has failed
		// more info: https://redis.io/topics/transactions
		err = fmt.Errorf("vdisk %q was busy and couldn't be modified", input.Target.VdiskID)
	}

	return
}

func init() {
	DedupedCmd.Long = DedupedCmd.Short + `

When no target_url is given, the target_url is the same as the source_url.`
}
