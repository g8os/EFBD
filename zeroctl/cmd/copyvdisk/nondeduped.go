package copyvdisk

import (
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var nondedupedCfg struct {
	SourceDatabase int
	TargetDatabase int
}

// NondedupedCmd represents the nondeduped copy subcommand
var NondedupedCmd = &cobra.Command{
	Use:   "nondeduped source_vdisk target_vdisk source_url [target_url]",
	Short: "Copy the data (blocks) of a nondeduped vdisk",
	RunE:  copyNondeduped,
}

func copyNondeduped(cmd *cobra.Command, args []string) error {
	logLevel := log.ErrorLevel
	if config.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	// parse user input
	log.Info("parsing positional arguments...")
	input, err := parseUserInput(args)
	if err != nil {
		return err
	}

	// get ardb connections
	log.Info("get the redis connection(s)...")
	connA, connB, err := getARDBConnections(
		input,
		nondedupedCfg.SourceDatabase, nondedupedCfg.TargetDatabase)
	if err != nil {
		return err
	}

	log.Infof("copy vdisk %q as %q",
		input.Source.VdiskID, input.Target.VdiskID)

	if connB == nil {
		err = copyNondedupedSameConnection(
			input.Source.VdiskID, input.Target.VdiskID, connA)
	} else {
		err = copyNondedupedDifferentConnections(
			input.Source.VdiskID, input.Target.VdiskID, connA, connB)
	}
	if err != nil {
		return err
	}

	log.Infof("copied succesfully vdisk %q to vdisk %q",
		input.Source.VdiskID, input.Target.VdiskID)
	return nil
}

func copyNondedupedSameConnection(sourceID, targetID string, conn redis.Conn) (err error) {
	defer conn.Close()

	script := redis.NewScript(0, `
if #ARGV ~= 2 then
    local usage = "copyNondedupedSameConnection script usage: source destination"
    redis.log(redis.LOG_NOTICE, usage)
    error("copyNondedupedSameConnection script requires 2 arguments (source destination)")
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

	log.Infof("dumping vdisk %q and restoring it as vdisk %q",
		sourceID, targetID)

	indexCount, err := redis.Int64(script.Do(conn, sourceID, targetID))
	if err == nil {
		log.Infof("copied %d block indices to vdisk %q",
			indexCount, targetID)
	}

	return
}

func copyNondedupedDifferentConnections(sourceID, targetID string, connA, connB redis.Conn) (err error) {
	defer func() {
		connA.Close()
		connB.Close()
	}()

	// get data from source connection
	log.Infof("collecting all data from source vdisk %q...", sourceID)
	data, err := redis.StringMap(connA.Do("HGETALL", sourceID))
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = fmt.Errorf("%q does not exist", sourceID)
		return
	}
	log.Infof("collected %d block indices from source vdisk %q",
		len(data), sourceID)

	// ensure the vdisk isn't touched while we're creating it
	if err = connB.Send("WATCH", targetID); err != nil {
		return
	}

	// start the copy transaction
	if err = connB.Send("MULTI"); err != nil {
		return
	}

	// delete any existing vdisk
	if err = connB.Send("DEL", targetID); err != nil {
		return
	}

	// buffer all data on target connection
	log.Infof("buffering %d block indices for target vdisk %q...",
		len(data), targetID)
	var index int64
	for rawIndex, content := range data {
		index, err = strconv.ParseInt(rawIndex, 10, 64)
		if err != nil {
			return
		}

		connB.Send("HSET", targetID, index, []byte(content))
	}

	// send all data to target connection (execute the transaction)
	log.Infof("flushing buffered data for target vdisk %q...", targetID)
	response, err := connB.Do("EXEC")
	if err == nil && response == nil {
		// if response == <nil> the transaction has failed
		// more info: https://redis.io/topics/transactions
		err = fmt.Errorf("vdisk %q was busy and couldn't be modified", targetID)
	}

	return
}

func init() {
	DedupedCmd.Long = DedupedCmd.Short + `

When no target_url is given, the target_url is the same as the source_url.`

	NondedupedCmd.Flags().IntVar(
		&nondedupedCfg.SourceDatabase,
		"sourcedb", 0,
		"database to use for the source connection (0 by default)")
	NondedupedCmd.Flags().IntVar(
		&nondedupedCfg.TargetDatabase,
		"targetdb", 0,
		"database to use for the target connection (0 by default)")
}
