package jobs

import (
	"math/rand"
	"testing"
)

type pushPayload struct {
	data     string
	priority uint32
}
type pushTest struct {
	queue                 string
	pushOrderWithPriority []pushPayload
	expectationOrder      []string
}

type pushPopTest struct {
	queue    string
	sequence []string
	pushes   []pushPayload
	pops     []string
}

var concurrentPushPopTests = [][]pushTest{
	{{"test", []pushPayload{{"foo", 0}, {"bar", 1}, {"baz", 1}, {"zoiks", 1}}, []string{"bar", "baz", "zoiks", "foo"}}},
	{
		{"test1", []pushPayload{{"foo", 0}, {"bar", 1}, {"baz", 1}, {"zoiks", 1}}, []string{"bar", "baz", "zoiks", "foo"}},
		{"test2", []pushPayload{{"a", 0}, {"b", 1}, {"c", 3}, {"d", 2}, {"e", 1}, {"f", 4}, {"g", 0}}, []string{"f", "c", "d", "b", "e", "a", "g"}},
	},
}

var sequentialPushPopTests = []pushPopTest{
	{"test1", []string{"push", "push", "pop"}, []pushPayload{{"a", 1}, {"b", 0}}, []string{"a"}},
	{"test2", []string{"push", "push", "pop", "push", "pop", "pop"}, []pushPayload{{"a", 0}, {"b", 1}, {"c", 1}}, []string{"b", "c", "a"}},
}

func TestSortedPriorityQueue_BasicPushPop(t *testing.T) {
	for _, test := range concurrentPushPopTests {
		store := NewMemoryStore(nil)
		queue, err := NewSortedPriorityQueue(store)
		if err != nil {
			t.Fatalf("Expected '%v' to be nil", err)
		}

		for _, part := range test {
			for _, job := range part.pushOrderWithPriority {
				if _, err := queue.Push(part.queue, Job{
					Data:     []byte(job.data),
					Priority: job.priority,
				}); err != nil {
					t.Errorf("Expected '%v' to be nil", err)
				}
			}
		}

		for _, part := range test {
			for _, expected := range part.expectationOrder {
				if err := queue.Pop(part.queue, func(job *Job) error {
					actual := string(job.Data)
					if actual != expected {
						t.Errorf("%s was not expected value: %s", actual, expected)
					}
					return nil
				}); err != nil {
					t.Errorf("Expected '%v' to be nil", err)
				}
			}
		}
	}

	for _, test := range sequentialPushPopTests {
		store := NewMemoryStore(nil)
		queue, err := NewSortedPriorityQueue(store)
		if err != nil {
			t.Fatalf("Expected '%v' to be nil", err)
		}

		for _, operation := range test.sequence {
			switch operation {
			case "push":
				var job pushPayload
				job, test.pushes = test.pushes[0], test.pushes[1:]
				if _, err := queue.Push(test.queue, Job{
					Data:     []byte(job.data),
					Priority: job.priority,
				}); err != nil {
					t.Errorf("Expected '%v' to be nil", err)
				}
			case "pop":
				var expected string
				expected, test.pops = test.pops[0], test.pops[1:]
				if err := queue.Pop(test.queue, func(job *Job) error {
					actual := string(job.Data)
					if actual != expected {
						t.Errorf("%s was not expected value: %s", actual, expected)
					}
					return nil
				}); err != nil {
					t.Errorf("Expected '%v' to be nil", err)
				}
			default:
				t.Fatalf("Unknown operation %s", operation)
			}
		}

		if len(test.pops) != 0 || len(test.pushes) != 0 {
			t.Fatal("Didn't run all tests")
		}
	}
}

type loadPushPopTest struct {
	initial  map[string][]byte
	queue    string
	sequence []string
	pushes   []pushPayload
	pops     []string
}

var loadPushPopTests = []loadPushPopTest{
	{
		map[string][]byte{
			"test2|4|1509126255448444742|1": []byte("a"),
			"test2|3|1509126255448440400|1": []byte("b"),
			"test2|2|1509126255448441480|1": []byte("c"),
			"test2|1|1509126255448439392|1": []byte("d"),
			"test2|1|1509126255448439392|2": []byte("e"),
		},
		"test2", []string{"pop", "pop", "pop", "pop", "pop"}, []pushPayload{}, []string{"a", "b", "c", "d", "e"},
	},
	{
		map[string][]byte{
			"test2|4|1509126255448444742|1": []byte("a"),
			"test2|3|1509126255448440400|1": []byte("b"),
			"test2|2|1509126255448441480|1": []byte("c"),
			"test2|1|1509126255448439392|1": []byte("d"),
			"test2|1|1509126255448439392|2": []byte("e"),
		},
		"test2", []string{"pop", "push", "pop", "pop", "pop", "pop", "pop"}, []pushPayload{{"f", 2}}, []string{"a", "b", "c", "f", "d", "e"},
	},
}

func TestSortedPriorityQueue_LoadPushPop(t *testing.T) {
	for _, test := range loadPushPopTests {
		store := NewMemoryStore(test.initial)
		queue, err := NewSortedPriorityQueue(store)
		if err != nil {
			t.Fatalf("Expected '%v' to be nil", err)
		}

		for _, operation := range test.sequence {
			switch operation {
			case "push":
				var job pushPayload
				job, test.pushes = test.pushes[0], test.pushes[1:]
				if _, err := queue.Push(test.queue, Job{
					Data:     []byte(job.data),
					Priority: job.priority,
				}); err != nil {
					t.Errorf("Expected '%v' to be nil", err)
				}
			case "pop":
				var expected string
				expected, test.pops = test.pops[0], test.pops[1:]
				if err := queue.Pop(test.queue, func(job *Job) error {
					actual := string(job.Data)
					if actual != expected {
						t.Errorf("%s was not expected value: %s", actual, expected)
					}
					return nil
				}); err != nil {
					t.Errorf("Expected '%v' to be nil", err)
				}
			default:
				t.Fatalf("Unknown operation %s", operation)
			}
		}

		if len(test.pops) != 0 || len(test.pushes) != 0 {
			t.Fatal("Didn't run all tests")
		}
	}
}

func BenchmarkPush_Memory(b *testing.B) {
	store := NewMemoryStore(nil)
	defer store.Close()

	queue, err := NewSortedPriorityQueue(store)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	for n := 0; n < b.N; n++ {
		if _, err := queue.Push("benchmark", Job{
			Data:     []byte{},
			Priority: uint32(rand.Intn(10)),
		}); err != nil {
			b.Fatalf("Error while pushing: %v", err)
		}
	}
}

func BenchmarkPop_Memory(b *testing.B) {
	store := NewMemoryStore(nil)
	defer store.Close()

	queue, err := NewSortedPriorityQueue(store)
	if err != nil {
		b.Fatalf("Error while making queue: %v", err)
	}

	for n := 0; n < b.N; n++ {
		if _, err := queue.Push("benchmark", Job{
			Data:     []byte{},
			Priority: uint32(rand.Intn(10)),
		}); err != nil {
			b.Fatalf("Error while pushing: %v", err)
		}
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		if err := queue.Pop("benchmark", func(_ *Job) error { return nil }); err != nil {
			b.Fatalf("Error while popping: %v", err)
		}
	}
}
