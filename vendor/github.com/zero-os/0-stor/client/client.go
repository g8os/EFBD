package client

import (
	"errors"
	"io"

	"github.com/zero-os/0-stor/client/config"
	"github.com/zero-os/0-stor/client/itsyouonline"
	"github.com/zero-os/0-stor/client/lib/block"
	"github.com/zero-os/0-stor/client/lib/chunker"
	"github.com/zero-os/0-stor/client/meta"
	"github.com/zero-os/0-stor/client/pipe"
)

var (
	errWriteFChunkerOnly = errors.New("WriteF only support chunker as first pipe")
	errReadFChunkerOnly  = errors.New("ReadF only support chunker as first pipe")
)

var _ (itsyouonline.NamespaceManager) = (*Client)(nil) // build time check that we implement stor.NamespaceManager interface

// Client defines 0-stor client
type Client struct {
	conf                          *config.Config
	metaCli                       *meta.Client
	itsyouonline.NamespaceManager //implement the NamespaceManager interface

	storWriter block.Writer
	storReader block.Reader
}

// New creates new client from the given config
func New(conf *config.Config) (*Client, error) {
	// append stor client to the end of pipe if needed
	conf.CheckAppendStorClient()

	return newClient(conf)
}

func newClient(conf *config.Config) (*Client, error) {
	// stor writer
	storWriter, err := pipe.NewWritePipe(conf, block.NewNilWriter(), nil)
	if err != nil {
		return nil, err
	}

	// stor reader
	storReader, err := pipe.NewReadPipe(conf)
	if err != nil {
		return nil, err
	}

	client := Client{
		conf:       conf,
		storWriter: storWriter,
		storReader: storReader,
	}

	if len(conf.MetaShards) > 0 {
		// meta client
		metaCli, err := meta.NewClient(conf.MetaShards)
		if err != nil {
			return nil, err
		}
		client.metaCli = metaCli
	}

	client.NamespaceManager = itsyouonline.NewClient(conf.Organization, conf.IYOAppID, conf.IYOSecret)

	return &client, nil
}

// Close the client
func (c *Client) Close() error {
	if c.metaCli != nil {
		c.metaCli.Close()
	}
	return nil
}

// WriteF writes the key with value taken from given io.Reader
// it currently only support `chunker` as first pipe.
// Metadata linked list will be build if prevKey is not nil.
// prevMeta is optional previous metadata, to be used in case of user already has the prev meta.
// So the client won't need to fetch it back from the metadata server.
// prevKey still need to be set it prevMeta is set
// initialMeta is optional metadata, if user want to set his own initial metadata for example set own epoch
func (c *Client) WriteF(key []byte, r io.Reader, prevKey []byte, prevMeta, initialMeta *meta.Meta) (*meta.Meta, error) {
	if !c.conf.ChunkerFirstPipe() {
		return nil, errWriteFChunkerOnly
	}

	wp, err := pipe.NewWritePipe(c.conf, block.NewNilWriter(), r)
	if err != nil {
		return nil, err
	}

	return c.doWrite(wp, key, nil, prevKey, prevMeta, initialMeta)
}

// Write writes the key-value to the configured pipes.
// Metadata linked list will be build if prevKey is not nil
// prevMeta is optional previous metadata, to be used in case of user already has the prev meta.
// So the client won't need to fetch it back from the metadata server.
// prevKey still need to be set it prevMeta is set
// initialMeta is optional metadata, if user want to set his own initial metadata for example set own epoch
func (c *Client) Write(key, val []byte, prevKey []byte, prevMeta, initialMeta *meta.Meta) (*meta.Meta, error) {
	return c.doWrite(c.storWriter, key, val, prevKey, prevMeta, initialMeta)
}

func (c *Client) doWrite(writer block.Writer, key, val []byte, prevKey []byte,
	prevMd *meta.Meta, md *meta.Meta) (*meta.Meta, error) {

	var err error

	if len(prevKey) > 0 && prevMd == nil {
		// get the prev meta now than later
		// to avoid making processing and then
		// just found that prev meta is invalid
		prevMd, err = c.metaCli.Get(string(prevKey))
		if err != nil {
			return nil, err
		}
	}

	// create new metadata which we want to pass through
	// to the pipe
	if md == nil {
		md, err = meta.New(key, 0, nil)
		if err != nil {
			return nil, err
		}
	}

	// process the data through the pipe
	md, err = writer.WriteBlock(key, val, md)
	if err != nil {
		return md, err
	}

	return md, c.linkMeta(md, prevMd, key, prevKey)

}

// ReadF read the key and write the result to the given io.Writer
// it currently only support `chunker` as first pipe
func (c *Client) ReadF(key []byte, w io.Writer) ([]byte, error) {
	if !c.conf.ChunkerFirstPipe() {
		return nil, errReadFChunkerOnly
	}

	readPipe, err := pipe.NewReadPipe(c.conf)
	if err != nil {
		return nil, err
	}

	rp0 := readPipe.Readers[len(readPipe.Readers)-1]
	chunkReader := rp0.(*chunker.BlockReader)
	return nil, chunkReader.Restore(key, w, readPipe)

}

// Read reads value with given key from the configured pipes.
func (c *Client) Read(key []byte) ([]byte, error) {
	return c.storReader.ReadBlock(key)
}

func (c *Client) linkMeta(curMd, prevMd *meta.Meta, curKey, prevKey []byte) error {
	if len(prevKey) == 0 {
		return nil
	}

	// point next key of previous meta to new meta
	if err := prevMd.SetNext(curKey); err != nil {
		return err
	}

	// point prev key of new meta to previous one
	if err := curMd.SetPrevious(prevKey); err != nil {
		return err
	}

	// update prev meta
	if err := c.metaCli.Put(string(prevKey), prevMd); err != nil {
		return err
	}

	// update new meta
	return c.metaCli.Put(string(curKey), curMd)
}
