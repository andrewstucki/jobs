# Jobs

This package implements a priority queue with optional persistent storage layers under
the `bolt` and `level` subpackages.

## Design

The package exposes a queuing interface with two simple commands: `Push` and `Pop`.

`Push` allows a job to be pushed onto a kv-store-backed queue with a given priority.

`Pop` takes a callback function used to process a job. If the callback returns an error
then the job is kept on the queue, if no error is returned, then the job is popped off
the queue and removed from the backing kv-store.

The default implementation of the queue uses ideas found in ["Implent Queue Service Using RocksDB"](https://github.com/facebook/rocksdb/wiki/Implement-Queue-Service-Using-RocksDB)

Namely, the default implementation generates a unique identifier for the job item with
the following key pattern `<queue>|<priority>|<unique_sequence>`, where `unique_sequence`
is a kind of [flake id](http://yellerapp.com/posts/2015-02-09-flake-ids.html) that is
comprised of a timestamp at which the underlying priority tracking mechanism was created
in memory and a counter--this we can garbage collect instances of the structures used for
tracking key identifiers/priorities and making jobs retrievable in O(1) time.

Basically the general structure of the algorithm is as follows:

1) Store a map of key-value pairs for the queues being tracked as (queue, priority-queue)
2) Store a list of "priority" references in the queue, each reference contains the current
and last sequence id for keys of the given priority.
  a) The priority references are stored in sorted order
3) Getting the next job is a matter of doing an O(1) lookup on the map for the right priority-queue,
doing an O(1) retrieval of the first element in the queue. And then using the current sequence id in the
priority reference to reconstruct the key for the job and using KV lookup semantics to retrieve the results in
O(1) time.
4) Inserting a job requires O(log n) time and has the following strategy:
  a) Lookup the priority reference list for the given queue
  b) Look at the priority of the job and search for the priority reference with the closest priority using
  a binary search
  c) If the found reference has the same priority as the job, then increment the last sequence on the counter
  and insert a record in the KV store with the given generated key
  d) If the found reference does not have the same priority as the job, create a new priority reference and
  insert it in the proper location in the priority references list.

## Examples

Examples of usage can be found in the tests or the examples directory, but the API is generally very simple:

    store := jobs.NewMemoryStore(nil)                // or bolt store or levelDB store
    queue, err := jobs.NewSortedPriorityQueue(store) // this re-creates the in-memory queue structure if the DB is persistent
    if err != nil {
      // handle errors and exit
    }

    // PUSHING

    data := []byte("Define your data here")          // the data for the job
    priority := uint32(rand.Intn(10))                // the priority

    jid, err := queue.Push("queue_name", jobs.Job{
			Data:     data,
			Priority: priority,
		})
    if err != nil {
      // handle errors
    }
    fmt.Printf("Enqueued Job - %s\n", jid)

    // POPPING

    if err := queue.Pop("queue_name", func(job *jobs.Job) error {
      // handle the job
      if job != nil {
        fmt.Printf("Popped Job - %s - had payload: %s\n", job.ID, job.Data)
      }
      return nil
    }); err != nil {
      // handle errors
    }
