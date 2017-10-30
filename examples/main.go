package main

import (
	"log"
	"math/rand"

	"github.com/andrewstucki/jobs"
)

func main() {
	store := jobs.NewMemoryStore(nil)                // or bolt store or levelDB store
	queue, err := jobs.NewSortedPriorityQueue(store) // this re-creates the in-memory queue structure if the DB is persistent
	if err != nil {
		log.Fatalf("Received error initializing: %v", err)
	}

	// PUSHING

	data := []byte("Define your data here") // the data for the job
	priority := uint32(rand.Intn(10))       // the priority

	jid, err := queue.Push("queue_name", jobs.Job{
		Data:     data,
		Priority: priority,
	})
	if err != nil {
		log.Fatalf("Received error pushing: %v", err)
	}
	log.Printf("Enqueued Job - %s\n", jid)

	// POPPING

	if err := queue.Pop("queue_name", func(job *jobs.Job) error {
		// handle the job
		if job != nil {
			log.Printf("Popped Job - %s - had payload: %s\n", job.ID, job.Data)
		}
		return nil
	}); err != nil {
		log.Fatalf("Received error popping: %v", err)
	}
}
