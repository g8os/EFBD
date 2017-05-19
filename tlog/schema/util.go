package schema

// RawTlogRespLen returns length of raw TlogResponse packet
func RawTlogRespLen(numSeq int) int {
	return 1 + // status:uint8
		(numSeq * 8) + // sequences:uint64
		40 // capnp overhead. TODO : find the exact number.
}

// RawServerHandshakeLen returns length of raw Handshake packet
func RawServerHandshakeLen() int {
	return 1 /*status:uint8*/ + 4 /*version:uint32*/ +
		40 // capnp overhead. TODO : find the exact number
}

// CopyBlock copies content of block 'src' to 'dst'
func CopyBlock(dst, src *TlogBlock) error {
	dst.SetSequence(src.Sequence())
	dst.SetLba(src.Lba())
	dst.SetSize(src.Size())

	hash, err := src.Hash()
	if err != nil {
		return err
	}
	dst.SetHash(hash)

	data, err := src.Data()
	if err != nil {
		return err
	}
	dst.SetData(data)

	dst.SetTimestamp(src.Timestamp())
	dst.SetOperation(src.Operation())
	return nil
}
