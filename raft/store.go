package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb"
	// "github.com/syndtr/goleveldb/leveldb"
)

var LeaderError = errors.New("not leader")

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type Command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

type RaftStoreConfig struct {
	EnableSingle     bool
	StorageDirectory string
	Bind             string
	BindPort         uint16
	ID               string

	DB        *badger.DB
	LogOutput io.Writer
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type RaftStore struct {
	RaftStoreConfig

	BindAddr string

	rpcServer       net.Listener
	db              *badger.DB
	raft            *raft.Raft
	backupTimestamp uint64
}

// New returns a new Store.
func NewRaftStore(raftConfig RaftStoreConfig) (*RaftStore, error) {
	r := &RaftStore{
		RaftStoreConfig: raftConfig,
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(raftConfig.ID)
	config.LogOutput = raftConfig.LogOutput

	r.BindAddr = fmt.Sprintf("%s:%d", raftConfig.Bind, raftConfig.BindPort)
	addr, err := net.ResolveTCPAddr("tcp", r.BindAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(r.BindAddr, addr, 3, 10*time.Second, raftConfig.LogOutput)
	if err != nil {
		return nil, err
	}

	storage := filepath.Join(raftConfig.StorageDirectory, "data")
	if err := os.MkdirAll(storage, 0700); err != nil {
		return nil, err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(raftConfig.StorageDirectory, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create the log store and stable store.
	logStore, err := raftbolt.NewBoltStore(filepath.Join(raftConfig.StorageDirectory, "raft.db"))
	if err != nil {
		return nil, err
	}

	// Instantiate the Raft systems.
	node, err := raft.NewRaft(config, r, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	if raftConfig.EnableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				raft.Server{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		node.BootstrapCluster(configuration)
	}

	r.raft = node

	if r.RaftStoreConfig.DB == nil {
		opts := badger.DefaultOptions
		opts.Dir = storage
		opts.ValueDir = storage
		db, err := badger.Open(opts)
		if err != nil {
			return nil, err
		}

		r.RaftStoreConfig.DB = db
	}
	r.db = r.RaftStoreConfig.DB

	r.Start()

	return r, nil
}

func (r *RaftStore) Start() {
	initializer := make(chan bool)
	go r.Listen(fmt.Sprintf("%s:%d", r.Bind, r.BindPort+1), initializer)
	<-initializer
}

func (r *RaftStore) WaitForLeader() {
	<-r.raft.LeaderCh()
}

func (r *RaftStore) get(key string) ([]byte, error) {
	// strongly consistent reads
	if r.raft.State() != raft.Leader {
		return nil, LeaderError
	}

	var item *badger.Item
	var err error
	err = r.db.View(func(txn *badger.Txn) error {
		item, err = txn.Get([]byte(key))
		return err
	})
	if err != nil {
		return nil, err
	}
	return item.Value()
}

func (r *RaftStore) put(key string, value []byte) error {
	if r.raft.State() != raft.Leader {
		return LeaderError
	}

	operation, err := json.Marshal(&Command{
		Op:    "put",
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	return r.raft.Apply(operation, raftTimeout).Error()
}

// Delete deletes the given key.
func (r *RaftStore) delete(key string) error {
	if r.raft.State() != raft.Leader {
		return LeaderError
	}

	operation, err := json.Marshal(&Command{
		Op:  "delete",
		Key: key,
	})
	if err != nil {
		return err
	}

	return r.raft.Apply(operation, raftTimeout).Error()
}

func (r *RaftStore) keys() ([]string, error) {
	// strongly consistent reads
	if r.raft.State() != raft.Leader {
		return nil, LeaderError
	}

	keys := []string{}
	r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keys = append(keys, string(item.Key()))
		}
		return nil
	})
	return keys, nil
}

func (r *RaftStore) Close() {
	r.raft.Shutdown().Error()
	r.rpcServer.Close()
	r.db.Close()
}

func (r *RaftStore) Join(nodeID, addr string) error {
	return r.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0).Error()
}
