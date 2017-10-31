package ardb

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/zero-os/0-Disk/errors"
	"github.com/zero-os/0-Disk/nbd/ardb/command"
)

// StorageAction defines the interface of an
// action which can be aplied to an ARDB connection.
type StorageAction interface {
	// Do applies this StorageAction to a given ARDB connection.
	Do(conn Conn) (reply interface{}, err error)

	// Send buffers the StorageAction to be aplied to a given ARDB connection.
	Send(conn Conn) (err error)

	// KeysModified returns a list of keys that will be modified by this StorageAction.
	// False is returned in case no keys are modified by this StorageAction.
	KeysModified() ([]string, bool)
}

// Command creates a single command on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Command(ct command.Type, args ...interface{}) *StorageCommand {
	if len(args) == 0 {
		panic("no arguments given")
	}

	return &StorageCommand{
		Type:      ct,
		Arguments: args,
	}
}

// StorageCommand defines a structure which allows you
// to encapsulate a commandName and arguments,
// such that it can be (re)used as a StorageAction.
type StorageCommand struct {
	Type      command.Type
	Arguments []interface{}
}

// Do implements StorageAction.Do
func (cmd *StorageCommand) Do(conn Conn) (reply interface{}, err error) {
	if cmd == nil {
		return nil, errNoCommandDefined
	}

	return conn.Do(cmd.Type.Name, cmd.Arguments...)
}

// Send implements StorageAction.Send
func (cmd *StorageCommand) Send(conn Conn) error {
	if cmd == nil {
		return errNoCommandDefined
	}
	return conn.Send(cmd.Type.Name, cmd.Arguments...)
}

// KeysModified implements StorageAction.KeysModified
func (cmd *StorageCommand) KeysModified() ([]string, bool) {
	if cmd == nil || !cmd.Type.Write {
		return nil, false
	}

	switch key := cmd.Arguments[0].(type) {
	case string:
		return []string{key}, true
	case []byte:
		return []string{string(key)}, true
	default:
		panic(fmt.Sprintf("%[1]v (%[1]T) is not a supported ARDB command key", key))
	}
}

// Commands creates a slice of commands on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Commands(cmds ...StorageAction) *StorageCommands {
	if len(cmds) == 0 {
		panic("no commands given")
	}

	return &StorageCommands{commands: cmds}
}

// StorageCommands defines a structure which allows you
// to encapsulate a slice of commands (see: StorageCommand),
// such that it can be (re)used as a StorageAction.
type StorageCommands struct {
	commands []StorageAction
}

// Do implements StorageAction.Do
func (cmds *StorageCommands) Do(conn Conn) (replies interface{}, err error) {
	if cmds == nil || cmds.commands == nil {
		return nil, errNoCommandsDefined
	}

	// 1. send all commands
	for _, cmd := range cmds.commands {
		err = cmd.Send(conn)
		if err != nil {
			return nil, err
		}
	}

	// 2. flush all commands and receive all replies
	return conn.Do("")
}

// Send implements StorageAction.Send
func (cmds *StorageCommands) Send(conn Conn) (err error) {
	if cmds == nil {
		return nil
	}

	for _, cmd := range cmds.commands {
		err = cmd.Send(conn)
		if err != nil {
			return
		}
	}

	return nil
}

// KeysModified implements StorageAction.KeysModified
func (cmds *StorageCommands) KeysModified() (keys []string, ok bool) {
	if cmds == nil {
		return nil, false
	}

	var cmdKeys []string
	for _, cmd := range cmds.commands {
		cmdKeys, ok = cmd.KeysModified()
		if ok {
			keys = append(keys, cmdKeys...)
		}
	}

	ok = (keys != nil)
	return
}

// Script creates a single script on the fly,
// ready to be used as a(n) (ARDB) StorageAction.
func Script(keyCount int, src string, valueKeys []string, keysAndArgs ...interface{}) *StorageScript {
	return &StorageScript{
		Script:           redis.NewScript(keyCount, src),
		KeysAndArguments: keysAndArgs,
		ValueKeys:        valueKeys,
	}
}

// StorageScript defines a structure which allows you
// to encapsulate a lua script and arguments,
// such that it can be (re)used as a StorageAction.
type StorageScript struct {
	Script           *redis.Script
	KeysAndArguments []interface{}
	// ValueKeys is a list of keys which is written to the ARDB connection.
	ValueKeys []string // TODO: define this automatically
}

// Do implements StorageAction.Do
func (cmd *StorageScript) Do(conn Conn) (reply interface{}, err error) {
	if cmd == nil || cmd.Script == nil {
		return nil, errNoCommandDefined
	}

	return cmd.Script.Do(conn, cmd.KeysAndArguments...)
}

// Send implements StorageAction.Send
func (cmd *StorageScript) Send(conn Conn) error {
	if cmd == nil || cmd.Script == nil {
		return errNoCommandDefined
	}
	return cmd.Script.Send(conn, cmd.KeysAndArguments...)
}

// KeysModified implements StorageAction.KeysModified
func (cmd *StorageScript) KeysModified() ([]string, bool) {
	if cmd == nil || cmd.ValueKeys == nil {
		return nil, false
	}
	return cmd.ValueKeys, true
}

var (
	_ StorageAction = (*StorageCommand)(nil)
	_ StorageAction = (*StorageCommands)(nil)
	_ StorageAction = (*StorageScript)(nil)
)

// Various action-related errors returned by this file.
var (
	errNoCommandDefined  = errors.New("no ARDB command defined")
	errNoCommandsDefined = errors.New("no ARDB commands defined")
)
