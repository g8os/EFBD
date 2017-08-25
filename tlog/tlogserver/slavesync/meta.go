package slavesync

import (
	"bytes"
	"encoding/gob"
)

func (ss *slaveSyncer) getLastSyncedSeq() (uint64, error) {
	b, err := ss.metaCli.GetMeta(ss.lastSeqSyncedKey)
	if err != nil {
		return 0, err
	}

	if len(b) == 0 {
		return 0, nil
	}

	var lastSeq uint64

	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&lastSeq)
	return lastSeq, err
}

func (ss *slaveSyncer) setLastSyncedSeq(seq uint64) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(seq)
	if err != nil {
		return err
	}
	return ss.metaCli.SaveMeta(ss.lastSeqSyncedKey, buf.Bytes())
}
