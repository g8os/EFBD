package schema

// CopyBlock copies content of block 'src' to 'dst'
func CopyBlock(dst, src *TlogBlock) error {
	vdiskID, err := src.VdiskID()
	if err != nil {
		return err
	}
	dst.SetVdiskID(vdiskID)

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
