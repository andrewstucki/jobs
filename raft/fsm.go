package raft

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
)

// Apply applies a Raft log entry to the key-value store.
func (r *RaftStore) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "put":
		return r.applyPut(c.Key, c.Value)
	case "delete":
		return r.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (r *RaftStore) Snapshot() (raft.FSMSnapshot, error) {
	return r, nil
}

// Restore stores the key-value store to a previous state.
func (r *RaftStore) Restore(rc io.ReadCloser) error {
	return r.db.Load(rc)
}

func (r *RaftStore) applyPut(key string, value []byte) interface{} {
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value, 0)
	})
}

func (r *RaftStore) applyDelete(key string) interface{} {
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}
