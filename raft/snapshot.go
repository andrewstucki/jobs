package raft

import (
	"github.com/hashicorp/raft"
)

func (r *RaftStore) Persist(sink raft.SnapshotSink) error {
	var err error
	var timestamp uint64
	err = func() error {
		timestamp, err = r.db.Backup(sink, r.backupTimestamp)
		if err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	r.backupTimestamp = timestamp

	return nil
}

func (r *RaftStore) Release() {}
