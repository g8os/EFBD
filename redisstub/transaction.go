package redisstub

import (
	"errors"
	"sync"
)

type command func(args ...interface{}) (reply interface{}, err error)

type cachedCommand struct {
	cmd  command
	args []interface{}
}

func newTransaction() *transaction {
	return new(transaction)
}

type transaction struct {
	commands []cachedCommand

	blockCommands []cachedCommand
	blockStarted  bool

	mux sync.Mutex
}

func (tx *transaction) Start() error {
	tx.mux.Lock()
	defer tx.mux.Unlock()

	if tx.blockStarted {
		return errors.New("MULTI blocks cannot be nested")
	}

	tx.blockStarted = true
	return nil
}

func (tx *transaction) Add(cmd command, expectedArgCount int, args ...interface{}) error {
	if length := len(args); length < expectedArgCount {
		return &InsufficientArgumentsError{expectedArgCount, length}
	}

	tx.mux.Lock()
	defer tx.mux.Unlock()

	tx.commands = append(tx.commands, cachedCommand{
		cmd:  cmd,
		args: args,
	})
	return nil
}

func (tx *transaction) Commit() (replies []interface{}, err error) {
	tx.mux.Lock()
	defer tx.mux.Unlock()

	if len(tx.commands) == 0 {
		err = errors.New("no cached commands available to commit")
		return
	}

	defer func() {
		tx.commands = nil
	}()

	var reply interface{}
	for _, cc := range tx.commands {
		reply, err = cc.cmd(cc.args...)
		if err != nil {
			replies = nil
			return
		}

		replies = append(replies, reply)
	}

	return
}

func (tx *transaction) Discard() error {
	tx.mux.Lock()
	defer tx.mux.Unlock()

	if !tx.blockStarted || len(tx.blockCommands) == 0 {
		return errors.New("no block commands available to discard")
	}

	tx.blockStarted = false
	tx.blockCommands = nil

	return nil
}

func (tx *transaction) Execute() (replies []interface{}, err error) {
	tx.mux.Lock()
	defer tx.mux.Unlock()

	if !tx.blockStarted || len(tx.blockCommands) == 0 {
		err = errors.New("no block commands available to execute")
		return nil, nil
	}

	defer func() {
		tx.blockStarted = false
		tx.blockCommands = nil
	}()

	var reply interface{}
	for _, cc := range tx.blockCommands {
		reply, err = cc.cmd(cc.args...)
		if err != nil {
			replies = nil
			return
		}

		replies = append(replies, reply)
	}

	return
}

func (tx *transaction) Clear() error {
	tx.mux.Lock()
	defer tx.mux.Unlock()

	tx.commands = nil

	tx.blockCommands = nil
	tx.blockStarted = false

	return nil
}
