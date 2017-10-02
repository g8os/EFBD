package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	valid "github.com/asaskevich/govalidator"
	"github.com/zeebo/bencode"
	"github.com/zero-os/0-Disk"
)

// LoadHeader loads (read=>[decrypt=>]decompress=>decode)
// a (snapshot) header from a given (backup) server.
func LoadHeader(id string, src StorageDriver, key *CryptoKey, ct CompressionType) (*Header, error) {
	buf := bytes.NewBuffer(nil)
	err := src.GetHeader(id, buf)
	if err != nil {
		return nil, err
	}
	header, err := deserializeHeader(key, ct, buf, decodeHeader)
	if err != nil && err != errInvalidHeaderDecode {
		return nil, err
	}
	if err == nil {
		// ensure the snapshot ID equals the given ID
		if header.Metadata.SnapshotID != id {
			return nil, errInvalidSnapshotID
		}
		return header, nil
	}

	// try to read again, and this time decode as a deduped map
	buf.Reset()
	err = src.GetHeader(id, buf)
	if err != nil {
		return nil, err
	}
	header, err = deserializeHeader(key, ct, buf, decodeRawDedupedMap)
	if err != nil {
		return nil, err
	}
	header.Metadata.SnapshotID = id

	// ok we could read the header directly as a deduped map,
	// now we need to get the block size, which we can do hacky,
	// by reading any block, as all blocks are supposed to be the same block size.
	if header.DedupedMap.Count == 0 {
		return nil, errNilSnapshot
	}
	firstBlock, err := readDedupedBlock(
		header.DedupedMap.Indices[0], header.DedupedMap.Hashes[0],
		src, key, ct)
	if err != nil {
		return nil, fmt.Errorf("couldn't read first deduped block of snapshot %s: %v", id, err)
	}
	header.Metadata.BlockSize = int64(len(firstBlock))
	return header, nil
}

func readDedupedBlock(index int64, hash zerodisk.Hash, src StorageDriver, key *CryptoKey, ct CompressionType) ([]byte, error) {
	decompressor, err := NewDecompressor(ct)
	if err != nil {
		return nil, err
	}

	var decrypter Decrypter
	if key.Defined() {
		decrypter, err = NewDecrypter(key)
		if err != nil {
			return nil, err
		}
	}

	hasher, err := newKeyedHasher(ct, *key)
	if err != nil {
		return nil, err
	}

	pipeline := &importPipeline{
		StorageDriver: src,
		Decrypter:     decrypter,
		Decompressor:  decompressor,
		Hasher:        hasher,
	}

	return pipeline.ReadBlock(index, hash)
}

// decoding-related errors.
var (
	errInvalidSnapshotID = errors.New("invalid snapshot ID")
	errNilSnapshot       = errors.New("snapshot contains no blocks")
)

// deserializeHeader allows you to deserialize a (snapshot) header from a given reader.
// It is expected that all the data in the reader is available,
// and is compressed and (only than optionally) encrypted.
// This function will attempt to decrypt (if needed) and decompress the read data,
// using (if given) the private (AES) key and compression type.
// The given compression type and (optional) private key has to match the information,
// used to serialize this Header in the first place.
func deserializeHeader(key *CryptoKey, ct CompressionType, src io.Reader, decoder func(src io.Reader) (*Header, error)) (*Header, error) {
	decompressor, err := NewDecompressor(ct)
	if err != nil {
		return nil, err
	}

	var bufB *bytes.Buffer

	if key.Defined() {
		bufA := bytes.NewBuffer(nil)

		err = Decrypt(key, src, bufA)
		if err != nil {
			return nil, fmt.Errorf("couldn't decrypt compressed header: %v", err)
		}

		bufB = bytes.NewBuffer(nil)
		err = decompressor.Decompress(bufA, bufB)
		if err != nil {
			return nil, fmt.Errorf("couldn't decompress header: %v", err)
		}
	} else {
		bufB = bytes.NewBuffer(nil)

		err = decompressor.Decompress(src, bufB)
		if err != nil {
			return nil, fmt.Errorf("couldn't decompress header: %v", err)
		}
	}

	return decoder(bufB)
}

// decodeHeader decodes the (snapshot) header from the given reader.
func decodeHeader(src io.Reader) (*Header, error) {
	var header Header
	err := bencode.NewDecoder(src).Decode(&header)
	if err != nil {
		return nil, errInvalidHeaderDecode
	}
	err = header.Validate()
	if err != nil {
		return nil, errInvalidHeaderDecode
	}
	return &header, nil
}

// decodeRawDedupedMap decodes the deduped map as a standalone structure.
// NOTE: This is for backwards compatibility only,
//       as the original backup module was storing the deduped map directly.
func decodeRawDedupedMap(src io.Reader) (*Header, error) {
	var raw RawDedupedMap
	err := bencode.NewDecoder(src).Decode(&raw)
	if err != nil {
		return nil, fmt.Errorf("couldn't decode bencoded deduped map: %v", err)
	}
	err = raw.Validate()
	if err != nil {
		return nil, err
	}
	return &Header{
		DedupedMap: raw,
	}, nil
}

var (
	errInvalidHeaderDecode = errors.New("couldn't decode bencoded header")
)

// StoreHeader StoreHeader (encode=>compress=>[encrypt=>]writes)
// a (snapshot) header to a given (backup) server.
func StoreHeader(header *Header, key *CryptoKey, ct CompressionType, dst StorageDriver) error {
	buf := bytes.NewBuffer(nil)
	err := serializeHeader(header, key, ct, buf)
	if err != nil {
		return err
	}
	return dst.SetHeader(header.Metadata.SnapshotID, buf)
}

// serializeHeader allows you to write all data of the header in a binary encoded manner,
// to the given writer. The encoded data will be compressed and optionally encrypted before being
// writen to the given writer.
// You can deserialize this header in memory using the `deserializeHeader` function.
func serializeHeader(header *Header, key *CryptoKey, ct CompressionType, dst io.Writer) error {
	compressor, err := NewCompressor(ct)
	if err != nil {
		return err
	}

	hmbuffer := bytes.NewBuffer(nil)
	err = encodeHeader(header, hmbuffer)
	if err != nil {
		return err
	}

	if key.Defined() {
		// compress and encrypt
		imbuffer := bytes.NewBuffer(nil)
		err = compressor.Compress(hmbuffer, imbuffer)
		if err != nil {
			return fmt.Errorf("couldn't compress bencoded dedupd map: %v", err)
		}

		err = Encrypt(key, imbuffer, dst)
		if err != nil {
			return fmt.Errorf("couldn't encrypt compressed dedupd map: %v", err)
		}
	} else {
		// only compress
		err = compressor.Compress(hmbuffer, dst)
		if err != nil {
			return fmt.Errorf("couldn't compress bencoded dedupd map: %v", err)
		}
	}

	return nil
}

// encodeHeader encodes the (snapshot) header to a given writer.
func encodeHeader(header *Header, dst io.Writer) error {
	err := bencode.NewEncoder(dst).Encode(header)
	if err != nil {
		return fmt.Errorf("couldn't bencode encode header: %v", err)
	}
	return nil
}

// Header stored for a snapshot (vdisk backup),
// this contains all the metadata and mapping from indices to blocks.
type Header struct {
	Metadata   Metadata      `bencode:"meta" valid:"optional"`
	DedupedMap RawDedupedMap `bencode:"map" valid:"required"`
}

// Validate this header,
// returning an error if it isn't valid.
func (header *Header) Validate() error {
	if header == nil {
		return errNilHeader
	}

	_, err := valid.ValidateStruct(header)
	if err != nil {
		return err
	}

	return header.DedupedMap.Validate()
}

// Metadata contains required and optional information about
// a snapshot (vdisk backup).
type Metadata struct {
	// required: ID of the snapshot (vdisk backup)
	SnapshotID string `bencode:"id" valid:"required"`
	// required: block size of the snapshot in KiB
	BlockSize int64 `bencode:"bs" valid:"required"`
	// optional: creation (RFC3339) data, time and timezone
	Created string `bencode:"at" valid:"optional"`
	// optional: information about the source
	// used to created the backup from.
	Source Source `bencode:"src" valid:"optional"`
}

// Source contains some optional metadata for a snapshot,
// describing the source vdisk used to create the snapshot.
type Source struct {
	// ID of the source vdisk
	VdiskID string `bencode:"id" valid:"optional"`
	// block size of the source vdisk in KiB
	BlockSize int64 `bencode:"bs" valid:"optional"`
}

// RawDedupedMap defines the structure used to
// encode a deduped map to a binary format.
// See https://github.com/zeebo/bencode for more information.
type RawDedupedMap struct {
	Count   int64    `bencode:"c" valid:"required"`
	Indices []int64  `bencode:"i" valid:"optional"`
	Hashes  [][]byte `bencode:"h" valid:"optional"`
}

// Validate this Raw Deduped Map,
// returning an error if it isn't valid.
func (rdm *RawDedupedMap) Validate() error {
	if rdm == nil {
		return errNilRawDedupedMap
	}

	_, err := valid.ValidateStruct(rdm)
	if err != nil {
		return err
	}

	if rdm.Count == 0 {
		return errInvalidNoHashes
	}
	if rdm.Count != int64(len(rdm.Indices)) {
		return errInvalidIndexCount
	}
	if rdm.Count != int64(len(rdm.Hashes)) {
		return errInvalidHashCount
	}

	return nil
}

var (
	errNilHeader         = errors.New("header is nil")
	errNilRawDedupedMap  = errors.New("nil raw deduped map")
	errInvalidNoHashes   = errors.New("deduped map contains no hashes, while this is required")
	errInvalidIndexCount = errors.New("invalid index count for decoded deduped map")
	errInvalidHashCount  = errors.New("invalid hash count for decoded deduped map")
)
