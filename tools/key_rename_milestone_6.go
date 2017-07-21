package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/config"
	"github.com/zero-os/0-Disk/nbdserver/ardb"
	"github.com/zero-os/0-Disk/nbdserver/lba"
)

func main() {
	// parse CLI flags and positional args
	err := parseFlagsAndArgs()
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		flag.Usage()
		return
	}

	var errs []string
	// go over all servers
	for _, serverCfg := range cfg.Servers {
		err = previewAndApplyChange(serverCfg)
		if err != nil {
			err = fmt.Errorf(
				"an error occured while processing '%s@%d': %v",
				serverCfg.Address, serverCfg.Database, err)
			fmt.Fprintln(os.Stderr, err)
			errs = append(errs, err.Error())
		}
	}

	// exit with non-0 status code if
	// at least one server couldn't be processed error-free
	// so let's notify user about these errors one last time
	if n := len(errs); n != 0 {
		fmt.Fprintf(os.Stderr,
			"\r\n an error occured in %d ardb server(s) while renaming:\r\n", n)
		fmt.Fprintln(os.Stderr, strings.Join(errs, "\r\n"))
		os.Exit(1)
	}
}

func init() {
	// register all optional flags
	flag.BoolVar(&cfg.Apply, "apply", false,
		"when specified, apply changes without asking first (default: false)")
	flag.Var(&cfg.VdiskType, "type",
		"define the type of vdisk you wish to rename relevant data for (default: boot)")
	flag.StringVar(&cfg.VdiskIDFilter, "regexp", "",
		"an optional regex which allows you to to process only the vdisks which ID match the given (re2) regexp (syntax docs: https://github.com/google/re2/wiki/Syntax) (disabled by default)")

	// improve usage message
	// (printed in case invalid input was given by the user at startup)
	flag.Usage = func() {
		var exe string
		if len(os.Args) > 0 {
			exe = path.Base(os.Args[0])
		} else {
			exe = "data_migration_milestone_5_to_6"
		}

		fmt.Fprintln(os.Stderr, `
This tool is meant to apply the (meta)data key renaming,
due to the fact that redis hashmap keys are prefixed since milstone 6,
while they weren't prefixed yet in milestone 5.

For more information about how all (meta)data is stored you can read:
https://github.com/zero-os/0-Disk/blob/master/docs/nbd/storage/storage.md

Executing this tool can take several minutes(!) to finish,
depending on the (total) amount of keys available
in the given server(s).

Warning:
    If the '-apply' flag is specified,
    or you give permission manually,
    your data will be altered for good,
    use this command with precaution!

    (it is guaranteed however that all renaming operations
    on a single server are either all applied,
    or not applied at all)

Warning:
    Currently this tool will rename all (filtered)
    legacy hashmaps found on a given redis server,
    even those that do not belong to the given vdisk type.

    Please open a feature request in case you require the migration of
    (meta)data on servers which have vdisks of multiple types. For example
    servers which have both db- and boot vdisks stored.
    You have been warned!
`)

		fmt.Fprintln(os.Stderr, "Usage:", exe, "[flags] storage_address...")
		flag.PrintDefaults()
	}
}

// list relevant keys from a given connection
func listKeys(conn redis.Conn) ([]string, error) {
	keys, err := redis.Strings(luaKeyScanScript.Do(conn))
	if err != nil {
		return nil, nil // nothing to do
	}

	// having no keys found is not an error,
	// as it might mean that all keys are already migrated
	if len(keys) == 0 {
		return nil, nil
	}

	// check if we need to add the user's filtering
	// on top of the prefix filtering we already do
	// (the prefix filtering guarantees that we only rename legacy keys)
	var filter func(string) bool
	if cfg.VdiskIDRegexp == nil {
		filter = func(key string) bool {
			return !knownKeyPrefixesRegex.MatchString(key)
		}
	} else {
		// add user's specified filter on top of the prefix filter
		filter = func(key string) bool {
			return !knownKeyPrefixesRegex.MatchString(key) &&
				cfg.VdiskIDRegexp.MatchString(key)
		}
	}

	// used to remove any duplicate ids,
	// as the redis SCAN command does not give any
	// guarantee related to retuning a key only once
	// see: https://redis.io/commands/scan
	dedup := make(map[string]struct{}, len(keys))

	var found bool
	var filteredKeys []string

	// dedup and filter keys
	for _, key := range keys {
		// ensure each key is unique
		if _, found = dedup[key]; found {
			continue
		}

		// add it to dedup map wheter it's valid or not
		// this way we can also skip a future filter
		// if we already saw that key anyhow
		dedup[key] = struct{}{}
		if filter(key) {
			filteredKeys = append(filteredKeys, key)
		}
	}

	// again, it could mean that all wanted keys have already been migrated
	if len(filteredKeys) == 0 {
		return nil, nil // nothing to do
	}

	// returns at least one key to be migrated
	return filteredKeys, nil
}

// print out the changes that will happen,
// than ask for permission (or do not if -apply flag was given),
// and apply changes in case permission or -apply flag was given.
func previewAndApplyChange(serverCfg config.StorageServerConfig) error {
	conn, err := redis.Dial("tcp", serverCfg.Address,
		redis.DialDatabase(serverCfg.Database))
	if err != nil {
		return fmt.Errorf("couldn't dial to the storage server: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(os.Stderr,
		"\r\nscanning ardb at %s@%d for keys...\r\n",
		serverCfg.Address, serverCfg.Database)

	// collect all keys (if any)
	keys, err := listKeys(conn)
	if err != nil {
		return err
	}

	// no keys found, nothing to do here
	if len(keys) == 0 {
		fmt.Fprintln(os.Stderr, "\r\nno keys found to rename...")
		return nil
	}

	// show preview of the renaming to be done
	for _, key := range keys {
		fmt.Fprintln(os.Stderr, key, "->", renameKey(key), "(preview)")
	}

	fmt.Fprintf(os.Stderr, "\r\n%d key(s) can be renamed... \r\n", len(keys))

	// rename all found keys if we have (explicit/implicit) permission by the user
	if askPermission(len(keys)) {
		fmt.Fprintf(os.Stderr, "\r\nrenaming %d key(s)... \r\n", len(keys))
		// `-apply` is given, nor permission is needed
		return applyChange(conn, keys)
	}

	// user doesn't want to rename, nothing to do here
	fmt.Fprintf(os.Stderr, "\r\ncancel renaming of the %d found key(s)... \r\n", len(keys))
	return nil
}

// small util function to ask permission of the user
// either because the `-apply` CLI flag was set,
// or because the user gives permission
// by giving a positive answer on our question.
func askPermission(n int) bool {
	if cfg.Apply {
		// `-apply` is given, no explicit permission is required
		return true
	}

	var response string
	// loop until user responds correctly
	for {
		fmt.Fprintf(os.Stderr, "rename the %d key(s) listed above [y/n]: ", n)
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

// rename all found (relevant) keys on a given server
// to the new milestone 6 prefixed format
func applyChange(conn redis.Conn, keys []string) error {
	// start multi process
	err := conn.Send("MULTI")
	if err != nil {
		return fmt.Errorf("could't start rename pipeline: %v", err)
	}

	// add one rename cmd per found key in the pipeline
	for index, key := range keys {
		fmt.Println(key, "->", renameKey(key))
		err = conn.Send("RENAME", key, renameKey(key))
		if err != nil {
			return fmt.Errorf(
				"couldn't add key (#%d) %s to the rename pipeline: %v",
				index, key, err)
		}
	}

	// execute the renaming pipeline and
	// wait for the answer of the ARDB server
	_, err = conn.Do("EXEC")
	if err != nil {
		// something went wrong,
		// thanks the the MULTI/EXEC ardb feature,
		// it is guaranteed that not a single key was renamed
		// at this point
		return fmt.Errorf(
			"couldn't rename %d key(s): %v", len(keys), err)
	}

	// all keys were renamed successfully
	return nil
}

// parse and apply all flags and positional arguments
func parseFlagsAndArgs() error {
	// parse flags
	flag.Parse()
	if cfg.VdiskType.t == 0 {
		cfg.VdiskType.t = config.VdiskTypeBoot
	}

	// set prefix
	switch cfg.VdiskType.t.StorageType() {
	case config.StorageDeduped:
		cfg.VdiskPrefix = lba.StorageKeyPrefix
	case config.StorageNonDeduped:
		cfg.VdiskPrefix = ardb.NonDedupedStorageKeyPrefix
	case config.StorageSemiDeduped:
		cfg.VdiskPrefix = ardb.SemiDedupBitMapKeyPrefix
	default:
		return fmt.Errorf(
			"no migration needs to be done for vdisk %s", cfg.VdiskType.t)
	}

	var err error

	// create filter regex if it was given by the user
	if cfg.VdiskIDFilter != "" {
		cfg.VdiskIDRegexp, err = regexp.Compile(cfg.VdiskIDFilter)
		if err != nil {
			return fmt.Errorf(
				"invalid vdisk ID filter regex was given: %v", err)
		}
	}

	// parse positional args

	args := flag.Args()
	if len(args) < 1 {
		return errors.New("at least one storage address has to be given")
	}

	var serverCfg config.StorageServerConfig

	// parse and validate all given storage servers
	for _, arg := range args {
		serverCfg, err = config.ParseStorageServerConfigString(arg)
		if err != nil {
			return err
		}

		cfg.Servers = append(cfg.Servers, serverCfg)
	}

	return nil
}

// The config for this tool,
// filled by the user via flags and positional args.
// Check out the init and parseFlagsAndArgs,
// to learn more about what each cfg property does,
// if not already clear from the name.
var cfg struct {
	Apply         bool
	VdiskType     vdiskType
	VdiskPrefix   string
	VdiskIDFilter string
	VdiskIDRegexp *regexp.Regexp
	Servers       []config.StorageServerConfig
}

// vdiskType wraps around config.VdiskType,
// as to turn it into a flag-enabled type
type vdiskType struct {
	t config.VdiskType
}

// String implements flag.Value.String
func (t *vdiskType) String() string {
	return t.t.String()
}

// Set implements flag.Value.Set
func (t *vdiskType) Set(s string) error {
	return t.t.SetString(strings.ToLower(s))
}

// util function which is used to rename all keys
func renameKey(key string) string {
	return cfg.VdiskPrefix + key
}

// list of prefixes we use since milestone 6
// for all hashmaps the NBD server stores,
// we collect them here so that we can make sure
// that no scanned key matches the regexp below,
// because if it does, the key already follows the new format,
// and does not have to be renamed!
var (
	knownKeyPrefixesRegex = regexp.MustCompile("^(" +
		strings.Join(knownKeyPrefixList, "|") +
		")")
	knownKeyPrefixList = []string{
		lba.StorageKeyPrefix,
		ardb.NonDedupedStorageKeyPrefix,
		ardb.SemiDedupBitMapKeyPrefix,
	}
)

// Redis Lua Scripts Used

// script used to scan through all keys,
// and list the ones that are relevant to us
var luaKeyScanScript = redis.NewScript(0, `
-- we only care about keys which are strings
-- and which represents a hashmap
local function keyFilter(key)
    -- only string keys are accepted (no hashes as key for example)
    if type(key) ~= "string" then
        return false
    end

    -- only if the key represents a hashmap it is accepted
    local type = redis.call("TYPE", key)
    type = type.ok or type
    return type == "hash"
end

-- start the SCAN process
local startCursor = "0"
local result = redis.call("SCAN", startCursor)
local cursor = result[1] -- used for next iteratation, unless it equals '0'
local batch = result[2]  -- contains scanned keys

-- variable that will hold all valid keys
local validKeys = {}

-- other local vars used in loop
local key

-- key process loop
repeat
    for i = 1, #batch do
        key = batch[i]
        if keyFilter(key) then
            table.insert(validKeys, key)
        end
    end

    -- scan the next available keys (if any)
    result = redis.call("SCAN", cursor)
    cursor = result[1] -- used for next iteratation, unless it equals '0'
    batch = result[2]  -- contains scanned keys
until cursor == startCursor

return validKeys
`)
