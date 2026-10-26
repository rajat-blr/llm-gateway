package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

type blockingSink struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	records []Record
	fail    bool
}

func (s *blockingSink) WriteUsage(ctx context.Context, r Record) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
	}
	if s.fail {
		return ErrUnavailable
	}
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
	return nil
}
func newSink() *blockingSink {
	return &blockingSink{started: make(chan struct{}), release: make(chan struct{})}
}
func TestBoundedQueueOverflowAndDrain(t *testing.T) {
	sink := newSink()
	worker := NewWorker(sink, 1)
	if !worker.Enqueue(Record{ID: "one"}) {
		t.Fatal("initial enqueue failed")
	}
	<-sink.started
	n := 5
	queued := Record{ID: "two", ClientInput: &n, Attempts: []Attempt{{Number: 1, InputTokens: &n}}}
	if !worker.Enqueue(queued) {
		t.Fatal("bounded queue rejected within capacity")
	}
	n = 99
	queued.Attempts[0].Number = 2
	if worker.Enqueue(Record{ID: "overflow", Attempts: []Attempt{{Number: 1}}}) {
		t.Fatal("overflow was not rejected")
	}
	close(sink.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if worker.Drain(ctx) != nil {
		t.Fatal("drain failed")
	}
	stats := worker.Stats()
	if stats.Written != 2 || stats.Dropped != 1 || stats.DroppedAttempts != 1 || stats.WriteFailures != 0 {
		t.Fatalf("bad stats %+v", stats)
	}
	if *sink.records[1].ClientInput != 5 || *sink.records[1].Attempts[0].InputTokens != 5 || sink.records[1].Attempts[0].Number != 1 {
		t.Fatal("queued record was not a snapshot")
	}
	if worker.Enqueue(Record{ID: "late"}) {
		t.Fatal("enqueue after close succeeded")
	}
}
func TestQueueWriteFailureAndShutdownDeadline(t *testing.T) {
	sink := newSink()
	worker := NewWorker(sink, 2)
	worker.Enqueue(Record{ID: "active"})
	<-sink.started
	worker.Enqueue(Record{ID: "queued-one"})
	worker.Enqueue(Record{ID: "queued-two"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if worker.Drain(ctx) != context.DeadlineExceeded {
		t.Fatal("drain ignored its deadline")
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("drain exceeded finite budget")
	}
	worker.Wait()
	stats := worker.Stats()
	if stats.WriteFailures != 1 || stats.Dropped != 3 || stats.Written != 0 {
		t.Fatalf("shutdown losses invisible: %+v", stats)
	}
	sink = newSink()
	sink.fail = true
	close(sink.release)
	worker = NewWorker(sink, 1)
	worker.Enqueue(Record{ID: "failed"})
	if worker.Drain(context.Background()) != nil {
		t.Fatal("drain failed")
	}
	if worker.Stats().WriteFailures != 1 || worker.Stats().Dropped != 1 {
		t.Fatal("write failure not counted")
	}
}
