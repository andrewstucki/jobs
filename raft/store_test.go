package raft

import (
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/andrewstucki/jobs"
)

func BenchmarkPush_Raft(b *testing.B) {
	store, err := NewRaftStore(RaftStoreConfig{
		EnableSingle:     true,
		StorageDirectory: "raft_push.db",
		Bind:             "127.0.0.1",
		BindPort:         8082,
		ID:               "push",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer store.Close()
	defer os.RemoveAll("raft_push.db")

	time.Sleep(2000 * time.Millisecond)

	client, err := NewRaftClient(store)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	queue, err := jobs.NewSortedPriorityQueue(client)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if _, err := queue.Push("benchmark", jobs.Job{
			Data:     []byte{},
			Priority: uint32(rand.Intn(10)),
		}); err != nil {
			b.Fatalf("Error while pushing: %v", err)
		}
	}
}

func BenchmarkPush_RaftMulti(b *testing.B) {
	storeOne, err := NewRaftStore(RaftStoreConfig{
		EnableSingle:     true,
		StorageDirectory: "raft_push.db1",
		Bind:             "127.0.0.1",
		BindPort:         8082,
		ID:               "push1",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer storeOne.Close()
	defer os.RemoveAll("raft_push.db1")

	time.Sleep(2000 * time.Millisecond)

	storeTwo, err := NewRaftStore(RaftStoreConfig{
		StorageDirectory: "raft_push.db2",
		Bind:             "127.0.0.1",
		BindPort:         8084,
		ID:               "push2",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer storeTwo.Close()
	defer os.RemoveAll("raft_push.db2")
	storeOne.Join(storeTwo.ID, storeTwo.BindAddr)

	storeThree, err := NewRaftStore(RaftStoreConfig{
		StorageDirectory: "raft_push.db3",
		Bind:             "127.0.0.1",
		BindPort:         8086,
		ID:               "push3",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer storeThree.Close()
	defer os.RemoveAll("raft_push.db3")
	storeOne.Join(storeThree.ID, storeThree.BindAddr)

	time.Sleep(2000 * time.Millisecond)

	client, err := NewRaftClient(storeThree)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	queue, err := jobs.NewSortedPriorityQueue(client)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()
			if _, err := queue.Push("benchmark", jobs.Job{
				Data:     []byte{},
				Priority: uint32(rand.Intn(10)),
			}); err != nil {
				b.Fatalf("Error while pushing: %v", err)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkPop_Raft(b *testing.B) {
	store, err := NewRaftStore(RaftStoreConfig{
		EnableSingle:     true,
		StorageDirectory: "raft_pop.db",
		Bind:             "127.0.0.1",
		BindPort:         8084,
		ID:               "pop",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer store.Close()
	defer os.RemoveAll("raft_pop.db")

	time.Sleep(2000 * time.Millisecond)

	client, err := NewRaftClient(store)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	queue, err := jobs.NewSortedPriorityQueue(client)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	for n := 0; n < b.N; n++ {
		if _, err := queue.Push("benchmark", jobs.Job{
			Data:     []byte{},
			Priority: uint32(rand.Intn(10)),
		}); err != nil {
			b.Fatalf("Error while pushing: %v", err)
		}
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := queue.Pop("benchmark", func(_ *jobs.Job) error { return nil }); err != nil {
			b.Fatalf("Error while popping: %v", err)
		}
	}
}

func BenchmarkPop_RaftMulti(b *testing.B) {
	storeOne, err := NewRaftStore(RaftStoreConfig{
		EnableSingle:     true,
		StorageDirectory: "raft_push.db1",
		Bind:             "127.0.0.1",
		BindPort:         8082,
		ID:               "push1",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer storeOne.Close()
	defer os.RemoveAll("raft_push.db1")

	time.Sleep(2000 * time.Millisecond)

	storeTwo, err := NewRaftStore(RaftStoreConfig{
		StorageDirectory: "raft_push.db2",
		Bind:             "127.0.0.1",
		BindPort:         8084,
		ID:               "push2",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer storeTwo.Close()
	defer os.RemoveAll("raft_push.db2")
	storeOne.Join(storeTwo.ID, storeTwo.BindAddr)

	storeThree, err := NewRaftStore(RaftStoreConfig{
		StorageDirectory: "raft_push.db3",
		Bind:             "127.0.0.1",
		BindPort:         8086,
		ID:               "push3",
		LogOutput:        ioutil.Discard,
	})
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer storeThree.Close()
	defer os.RemoveAll("raft_push.db3")
	storeOne.Join(storeThree.ID, storeThree.BindAddr)

	time.Sleep(2000 * time.Millisecond)

	client, err := NewRaftClient(storeThree)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	queue, err := jobs.NewSortedPriorityQueue(client)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	for n := 0; n < b.N; n++ {
		if _, err := queue.Push("benchmark", jobs.Job{
			Data:     []byte{},
			Priority: uint32(rand.Intn(10)),
		}); err != nil {
			b.Fatalf("Error while pushing: %v", err)
		}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()
			if err := queue.Pop("benchmark", func(_ *jobs.Job) error { return nil }); err != nil {
				b.Fatalf("Error while popping: %v", err)
			}
		}()
	}
	wg.Wait()
}
