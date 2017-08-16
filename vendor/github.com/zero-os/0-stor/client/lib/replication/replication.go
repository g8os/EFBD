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

// NewWriter creates new writer.
// The replication will be done in async way if async = true.
func NewWriter(writers []block.Writer, conf Config) *Writer {
	var maxFailed int
	if conf.Number > 0 && conf.Number < len(writers) {
		maxFailed = len(writers) - conf.Number
	}

	return &Writer{
		async:     conf.Async,
		maxFailed: maxFailed,
		writers:   writers,
	}
}

// Write writes data to underlying writer
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

func writeAsync(writers []block.Writer, key, data []byte, md *meta.Meta,
	maxFailed int) ([]block.Writer, *meta.Meta, error) {

	var wg sync.WaitGroup
	var mux sync.Mutex
	var errs []error
	var written int
	var failedWriters []block.Writer

	wg.Add(len(writers))

	for _, writer := range writers {
		go func(writer block.Writer) {
			defer wg.Done()

			mux.Lock()
			defer mux.Unlock()
			md, err := writer.WriteBlock(key, data, md)

			// call the lock here to protect `errs` & `written` var
			// which is global to this func

			if err != nil {
				errs = append(errs, err)
				failedWriters = append(failedWriters, writer)
				return
			}

			written += int(md.Size())
		}(writer)
	}

	wg.Wait()

	md.SetSize(uint64(written))
	if len(errs) > maxFailed {
		return failedWriters, md, Error{errs: errs}
	}

	return failedWriters, md, nil
}

func writeSync(writers []block.Writer, key, data []byte, md *meta.Meta,
	maxFailed int) ([]block.Writer, *meta.Meta, error) {
	var written int
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
		written += int(md.Size())
		md.SetSize(uint64(written))
	}
	return failedWriters, md, nil
}
