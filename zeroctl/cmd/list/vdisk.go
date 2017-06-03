package list

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/spf13/cobra"
	"github.com/zero-os/0-Disk/log"
	"github.com/zero-os/0-Disk/zeroctl/cmd/config"
)

var vdiskCfg struct {
	MetaDatabase int
}

// VdiskCmd represents the list vdisk subcommand
var VdiskCmd = &cobra.Command{
	Use:   "vdisks meta_url",
	Short: "List all vdisks available on the meta_url's ardb",
	RunE:  listVdisks,
}

func listVdisks(cmd *cobra.Command, args []string) error {
	argn := len(args)

	if argn < 1 {
		return errors.New("not enough arguments")
	}
	if argn > 1 {
		return errors.New("too many arguments")
	}

	metaURL := args[0]

	logLevel := log.ErrorLevel
	if config.Verbose {
		logLevel = log.InfoLevel
	}
	log.SetLevel(logLevel)

	log.Infof("connection to meta ardb at %s (db: %d)",
		metaURL, vdiskCfg.MetaDatabase)

	// open redis connection
	redisDatabase := redis.DialDatabase(vdiskCfg.MetaDatabase)
	conn, err := redis.Dial("tcp", metaURL, redisDatabase)
	if err != nil {
		return fmt.Errorf("couldn't connect to the ardb: %s", err.Error())
	}
	defer conn.Close()

	log.Infof("scanning for all available vdisks...")

	script := redis.NewScript(0, vdiskListScriptSource)
	cursor := startListCursor
	var output [][]byte

	var vdisks [][]byte
	var vdisksLength int

	// go through all available keys
	for {
		output, err = redis.ByteSlices(script.Do(conn, cursor))
		if err != nil {
			log.Error("aborting key scan due to an error: ", err)
			break
		}

		vdisksLength = len(output) - 1
		if vdisksLength > 0 {
			vdisks = append(vdisks, output[:vdisksLength-1]...)
		}

		cursor = output[vdisksLength]
		if bytes.Compare(startListCursor, cursor) == 0 {
			break
		}
	}

	log.Infof("printing the available and unique vdisks...")

	var ok bool
	var vdiskID string

	uniqueVdisks := make(map[string]struct{})
	for _, vdisk := range vdisks {
		vdiskID = string(vdisk)
		if _, ok = uniqueVdisks[vdiskID]; ok {
			continue // skip, already printed
		}
		uniqueVdisks[vdiskID] = struct{}{}
		fmt.Println(vdiskID)
	}

	return nil
}

func init() {
	VdiskCmd.Long = VdiskCmd.Short + `

WARNING: This command is very slow, and might take a while to finish!
  It might also decrease the performance of the ardb server
  in question, by locking the server down for each operation.
`

	VdiskCmd.Flags().IntVar(
		&vdiskCfg.MetaDatabase,
		"db", 0,
		"database to use for the ardb connection (0 by default)")
}

var startListCursor = []byte("0")

const vdiskListScriptSource = `
local cursor = ARGV[1]

local result = redis.call("SCAN", cursor)
local batch = result[2]

local key
local type

local output = {}

for i = 1, #batch do
	key = batch[i]

	-- only add hashmaps
	type = redis.call("TYPE", key)
	type = type.ok or type
	if type == "hash" then
		table.insert(output, key)
	end
end

cursor = result[1]
table.insert(output, cursor)

return output
`
