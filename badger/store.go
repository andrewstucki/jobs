package badger

import (
	"os"

	"github.com/andrewstucki/jobs"

	"github.com/dgraph-io/badger"
)

func NewBadgerStore(path string) (jobs.PersistentStore, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &badgerStore{
		db: db,
	}, nil
}

type badgerStore struct {
	db *badger.DB
}

func (b *badgerStore) Put(key string, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value, 0)
	})
}

func (b *badgerStore) Get(key string) ([]byte, error) {
	var item *badger.Item
	var err error
	err = b.db.View(func(txn *badger.Txn) error {
		item, err = txn.Get([]byte(key))
		return err
	})
	if err != nil {
		return nil, err
	}
	return item.Value()
}

func (b *badgerStore) Delete(key string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (b *badgerStore) Keys() ([]string, error) {
	keys := []string{}
	b.db.View(func(txn *badger.Txn) error {
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

func (b *badgerStore) Close() {
	b.db.Close()
}
