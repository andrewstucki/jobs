package bolt

import (
	"fmt"

	"github.com/andrewstucki/jobs"

	"github.com/boltdb/bolt"
)

func NewBoltStore(path string) (jobs.PersistentStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.Update(func(tx *bolt.Tx) error {
		// if we can't create it, it means it exists
		tx.CreateBucket([]byte("jobs"))
		return nil
	})
	return &boltStore{
		db: db,
	}, nil
}

type boltStore struct {
	db *bolt.DB
}

func (b *boltStore) Put(key string, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("jobs")).Put([]byte(key), value)
	})
}

func (b *boltStore) Get(key string) ([]byte, error) {
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		value = tx.Bucket([]byte("jobs")).Get([]byte(key))
		if value == nil {
			return fmt.Errorf("Key '%s' not found", key)
		}
		return nil
	})
	return value, err
}

func (b *boltStore) Delete(key string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("jobs")).Delete([]byte(key))
	})
}

func (b *boltStore) Keys() ([]string, error) {
	keys := []string{}
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("jobs")).ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	})
	return keys, err
}

func (b *boltStore) Close() {
	b.db.Close()
}
