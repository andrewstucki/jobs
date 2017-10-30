package jobs

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SortedPriorityQueue interface {
	// Adds a job to the given queue with a particular priority
	Push(queue string, job Job) (string, error)

	// Incrementer returns an error if the state of our priority queue should not get updated
	// pop will pass a nil job to incrementer if the given queue is empty
	Pop(queue string, incrementer func(job *Job) error) error
}

func makeKey(queue string, priority uint32, timestamp int64, sequence int64) string {
	return fmt.Sprintf("%s|%d|%d|%d", queue, priority, timestamp, sequence)
}

func parseKey(key string) (queue string, priority uint32, timestamp int64, sequence int64) {
	tags := strings.Split(key, "|")

	// just assume we always have 4 tokens and that they are always parsible
	// since this is only used internally, and if we have a bug that bad we should just panic
	queue = tags[0]
	priority64, _ := strconv.ParseUint(tags[1], 10, 32)
	priority = uint32(priority64)
	timestamp, _ = strconv.ParseInt(tags[2], 10, 64)
	sequence, _ = strconv.ParseInt(tags[3], 10, 64)
	return
}

// Load the keys for tracking
func NewSortedPriorityQueue(store PersistentStore) (SortedPriorityQueue, error) {
	trackers := make(map[string][]*priorityTracker)
	keys, err := store.Keys()
	if err != nil {
		return nil, err
	}

	s := &sortedPriorityQueue{
		trackerSets: trackers,
		store:       store,
	}

	for _, key := range keys {
		queue, priority, timestamp, sequence := parseKey(key)
		tracker := s.upsert(queue, priority, timestamp, sequence, sequence)

		// make sure we have the highest sequence (we use the negative since Next is negative) recorded in the tracker
		if tracker.Current > sequence {
			tracker.Current = sequence
		}
		if tracker.Last < sequence {
			tracker.Last = sequence
		}
	}

	return s, nil
}

type priorityTracker struct {
	Priority uint32

	// the timestamp that the tracker was created at
	// we use this in constructing keys so that they can still be unique
	Timestamp int64

	// a sequence tracker that counts towards zero
	Current int64

	// a sequence tracker that counts towards zero
	Last int64
}

type sortedPriorityQueue struct {
	// each value for the trackers map is a sorted set
	trackerSets map[string][]*priorityTracker
	mutex       sync.Mutex

	store PersistentStore
}

func (s *sortedPriorityQueue) upsert(queue string, priority uint32, timestamp, current, last int64) *priorityTracker {
	var tracker *priorityTracker

	if trackerSet, ok := s.trackerSets[queue]; !ok {
		tracker = &priorityTracker{
			Priority:  priority,
			Timestamp: timestamp,
			Current:   current,
			Last:      last,
		}
		s.trackerSets[queue] = []*priorityTracker{tracker}
	} else {
		maxIndex := sort.Search(len(trackerSet), func(i int) bool {
			return trackerSet[i].Priority <= priority
		})

		// we're the lowest priority
		if maxIndex == len(trackerSet) {
			tracker = &priorityTracker{
				Priority:  priority,
				Timestamp: timestamp,
				Current:   current,
				Last:      last,
			}
			trackerSet = append(trackerSet, tracker)
		} else {
			// maybe we found the tracker
			tracker = trackerSet[maxIndex]

			// it was a miss, so we need to insert a new priority tracker just before the one we found
			if tracker.Priority != priority {

				// it was a miss, so we need to insert a new priority tracker just before the one we found
				// see https://github.com/golang/go/wiki/SliceTricks#insert
				trackerSet = append(trackerSet, nil)
				copy(trackerSet[maxIndex+1:], trackerSet[maxIndex:])
				tracker = &priorityTracker{
					Priority:  priority,
					Timestamp: timestamp,
					Current:   current,
					Last:      last,
				}
				trackerSet[maxIndex] = tracker
			}
		}
		s.trackerSets[queue] = trackerSet
	}
	return tracker
}

func (s *sortedPriorityQueue) Push(queue string, job Job) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tracker := s.upsert(queue, job.Priority, time.Now().UnixNano(), 1, 0)
	tracker.Last++
	key := makeKey(queue, job.Priority, tracker.Timestamp, tracker.Last)
	err := s.store.Put(key, job.Data)
	if err != nil {
		return "", err
	}
	return key, nil
}

func (s *sortedPriorityQueue) Pop(queue string, processor func(job *Job) error) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	trackerSet, ok := s.trackerSets[queue]
	if !ok {
		// we aren't going to update anyway, so ignore the error
		processor(nil)
		return nil
	}

	tracker := trackerSet[0]
	key := makeKey(queue, tracker.Priority, tracker.Timestamp, tracker.Current)
	data, err := s.store.Get(key)
	if err != nil {
		return err
	}

	err = processor(&Job{
		Data:     data,
		Priority: tracker.Priority,
		ID:       key,
	})

	if err != nil {
		return err
	}

	if err := s.store.Delete(key); err != nil {
		return err
	}

	// increment the reference to Current
	tracker.Current++

	// are we on the last item?
	if tracker.Last < tracker.Current {
		// drop the tracker for this priority level
		s.trackerSets[queue] = trackerSet[1:]
	}

	return nil
}
