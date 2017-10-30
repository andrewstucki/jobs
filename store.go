package jobs

import (
	"fmt"
	"sync"
)

type PersistentStore interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Keys() ([]string, error)
	Close()
}

func NewMemoryStore(table map[string][]byte) PersistentStore {
	if table == nil {
		table = make(map[string][]byte)
	}
	return &memoryStore{
		table: table,
	}
}

type memoryStore struct {
	table map[string][]byte
	mutex sync.RWMutex
}

func (m *memoryStore) Put(key string, value []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.table[key] = value
	return nil
}

func (m *memoryStore) Get(key string) ([]byte, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if value, ok := m.table[key]; ok {
		return value, nil
	}
	return nil, fmt.Errorf("Key '%s' not found", key)
}

func (m *memoryStore) Delete(key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.table[key]; !ok {
		return fmt.Errorf("Key '%s' not found", key)
	}
	delete(m.table, key)
	return nil
}

func (m *memoryStore) Keys() ([]string, error) {
	keys := []string{}
	for key := range m.table {
		keys = append(keys, key)
	}
	return keys, nil
}

func (m *memoryStore) Close() {}
