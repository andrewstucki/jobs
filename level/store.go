package level

import (
	"github.com/andrewstucki/jobs"

	"github.com/syndtr/goleveldb/leveldb"
)

func NewLevelDBStore(path string) (jobs.PersistentStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &levelDBStore{
		db: db,
	}, nil
}

type levelDBStore struct {
	db *leveldb.DB
}

func (l *levelDBStore) Put(key string, value []byte) error {
	return l.db.Put([]byte(key), value, nil)
}

func (l *levelDBStore) Get(key string) ([]byte, error) {
	return l.db.Get([]byte(key), nil)
}

func (l *levelDBStore) Delete(key string) error {
	return l.db.Delete([]byte(key), nil)
}

func (l *levelDBStore) Keys() ([]string, error) {
	keys := []string{}
	iter := l.db.NewIterator(nil, nil)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Release()
	return keys, iter.Error()
}

func (l *levelDBStore) Close() {
	l.db.Close()
}
