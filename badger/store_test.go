package badger

import (
	"math/rand"
	"os"
	"testing"

	"github.com/andrewstucki/jobs"
)

func BenchmarkPush_Badger(b *testing.B) {
	store, err := NewBadgerStore("badger_push.db")
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer store.Close()
	defer os.RemoveAll("badger_push.db")

	queue, err := jobs.NewSortedPriorityQueue(store)
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
}

func BenchmarkPop_Badger(b *testing.B) {
	store, err := NewBadgerStore("badger_pop.db")
	if err != nil {
		b.Fatalf("Error while making database: %v", err)
	}
	defer store.Close()
	defer os.RemoveAll("badger_pop.db")

	queue, err := jobs.NewSortedPriorityQueue(store)
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
