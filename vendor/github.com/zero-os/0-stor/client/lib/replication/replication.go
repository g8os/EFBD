package replication

import (
	"sync"

	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/meta"
)

// A Writer is created by taking one input and specifying multiple outputs.
// All the data that comes in are replicated on all the configured outputs.
type Writer struct {
	async     bool
	maxFailed int // maximum number of writer which can be failed
	writers   []block.Writer
}

// Config defines replication's configuration
type Config struct {
	Async bool `yaml:"async"`

	// replication number.
	// the number of replications we want to create.
	// 0 means all available shards
	Number int `yaml:"number"`
}

// NumReplication returns number of replication that must be created
func (c Config) NumReplication(numWriter int) int {
	if c.Number <= 0 || c.Number > numWriter {
		return numWriter
	}
	return c.Number
}

// NewWriter creates new writer.
// The replication will be done in async way if async = true.
func NewWriter(writers []block.Writer, conf Config) *Writer {
	return &Writer{
		async:     conf.Async,
		maxFailed: len(writers) - conf.NumReplication(len(writers)),
		writers:   writers,
	}
}

// WriteBlock writes data to underlying writer
func (w *Writer) WriteBlock(key, data []byte, md *meta.Meta) (*meta.Meta, error) {
	if w.async {
		_, md, err := writeAsync(w.writers, key, data, md, w.maxFailed)
		return md, err
	}
	_, md, err := writeSync(w.writers, key, data, md, w.maxFailed)
	return md, err
}

func (w *Writer) Write(key, data []byte, md *meta.Meta) ([]block.Writer, *meta.Meta, error) {
	if w.async {
		return writeAsync(w.writers, key, data, md, w.maxFailed)
	}
	return writeSync(w.writers, key, data, md, w.maxFailed)
}

func writeAsync(writers []block.Writer, key, data []byte, md *meta.Meta, maxFailed int) ([]block.Writer, *meta.Meta, error) {

	var wg sync.WaitGroup
	var mux sync.Mutex
	var errs []error

	var failedWriters []block.Writer

	wg.Add(len(writers))

	for _, writer := range writers {
		go func(writer block.Writer) {
			defer wg.Done()

			mux.Lock()
			defer mux.Unlock()

			_, err := writer.WriteBlock(key, data, md)

			// call the lock here to protect `errs` & `written` var
			// which is global to this func

			if err != nil {
				errs = append(errs, err)
				failedWriters = append(failedWriters, writer)
				return
			}
		}(writer)
	}

	wg.Wait()

	// md.Size += uint64(written)
	if len(errs) > maxFailed {
		return failedWriters, md, Error{errs: errs}
	}

	chunk := md.GetChunk(key)
	chunk.Size = uint64(len(data))

	return failedWriters, md, nil
}

func writeSync(writers []block.Writer, key, data []byte, md *meta.Meta, maxFailed int) ([]block.Writer, *meta.Meta, error) {

	var failedWriters []block.Writer
	var failedNum int

	for _, writer := range writers {
		md, err := writer.WriteBlock(key, data, md)
		if err != nil {
			failedNum++
			failedWriters = append(failedWriters, writer)
			if failedNum > maxFailed {
				return failedWriters, md, err
			}
		}
	}

	chunk := md.GetChunk(key)
	chunk.Size = uint64(len(data))

	return failedWriters, md, nil
}
