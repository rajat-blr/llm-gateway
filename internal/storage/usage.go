package storage

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

type Attempt struct {
	Number                    int
	Provider                  string
	StartedMS, DurationMS     int64
	Outcome                   string
	ReservedTokens            int64
	InputTokens, OutputTokens *int
	AccountingError           bool
}
type Record struct {
	ID, KeyID, Alias, Provider, Outcome string
	StartedMS, DurationMS               int64
	ClientInput, ClientOutput           *int
	Attempts                            []Attempt
}

func (s *Store) WriteUsage(ctx context.Context, r Record) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return ErrUnavailable
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO requests(id,key_id,alias,provider,started_ms,duration_ms,outcome,client_input,client_output) VALUES(?,?,?,?,?,?,?,?,?)`, r.ID, r.KeyID, r.Alias, r.Provider, r.StartedMS, r.DurationMS, r.Outcome, r.ClientInput, r.ClientOutput)
	if err != nil {
		return ErrUnavailable
	}
	for _, a := range r.Attempts {
		_, err = tx.ExecContext(ctx, `INSERT INTO attempts(request_id,number,provider,started_ms,duration_ms,outcome,reserved_tokens,input_tokens,output_tokens,accounting_error) VALUES(?,?,?,?,?,?,?,?,?,?)`, r.ID, a.Number, a.Provider, a.StartedMS, a.DurationMS, a.Outcome, a.ReservedTokens, a.InputTokens, a.OutputTokens, a.AccountingError)
		if err != nil {
			return ErrUnavailable
		}
	}
	if tx.Commit() != nil {
		return ErrUnavailable
	}
	return nil
}

type UsageSink interface {
	WriteUsage(context.Context, Record) error
}
type Worker struct {
	sink                                                UsageSink
	queue                                               chan Record
	mu                                                  sync.RWMutex
	closed                                              bool
	ctx                                                 context.Context
	cancel                                              context.CancelFunc
	done                                                chan struct{}
	enqueued, written, dropped, failed, droppedAttempts atomic.Uint64
	inFlight                                            atomic.Bool
}
type Stats struct {
	InFlight        bool   `json:"in_flight"`
	QueueDepth      int    `json:"queue_depth"`
	QueueCapacity   int    `json:"queue_capacity"`
	Enqueued        uint64 `json:"enqueued_records"`
	Written         uint64 `json:"written_records"`
	Dropped         uint64 `json:"dropped_records"`
	WriteFailures   uint64 `json:"write_failures"`
	DroppedAttempts uint64 `json:"dropped_attempts"`
}

func NewWorker(sink UsageSink, capacity int) *Worker {
	if capacity < 1 || capacity > 10000 {
		panic("invalid usage queue capacity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{sink: sink, queue: make(chan Record, capacity), ctx: ctx, cancel: cancel, done: make(chan struct{})}
	go w.run()
	return w
}
func (w *Worker) drop(r Record) {
	n := w.dropped.Add(1)
	w.droppedAttempts.Add(uint64(len(r.Attempts)))
	if n == 1 || n&(n-1) == 0 {
		slog.Warn("usage records dropped", "dropped_records", n)
	}
}
func (w *Worker) Enqueue(r Record) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		w.drop(r)
		return false
	}
	// Records contain metadata only. Copy the bounded attempt slice so the
	// producer cannot change a queued request's attempt ownership.
	r.Attempts = append([]Attempt(nil), r.Attempts...)
	clone := func(n *int) *int {
		if n == nil {
			return nil
		}
		v := *n
		return &v
	}
	r.ClientInput = clone(r.ClientInput)
	r.ClientOutput = clone(r.ClientOutput)
	for i := range r.Attempts {
		r.Attempts[i].InputTokens = clone(r.Attempts[i].InputTokens)
		r.Attempts[i].OutputTokens = clone(r.Attempts[i].OutputTokens)
	}
	select {
	case w.queue <- r:
		w.enqueued.Add(1)
		return true
	default:
		w.drop(r)
		return false
	}
}
func (w *Worker) run() {
	defer close(w.done)
	for r := range w.queue {
		if w.ctx.Err() != nil {
			w.drop(r)
			continue
		}
		w.inFlight.Store(true)
		if err := w.sink.WriteUsage(w.ctx, r); err != nil {
			w.failed.Add(1)
			w.drop(r)
			slog.Error("usage persistence failed")
		} else {
			w.written.Add(1)
		}
		w.inFlight.Store(false)
	}
}
func (w *Worker) Stats() Stats {
	return Stats{InFlight: w.inFlight.Load(), QueueDepth: len(w.queue), QueueCapacity: cap(w.queue), Enqueued: w.enqueued.Load(), Written: w.written.Load(), Dropped: w.dropped.Load(), WriteFailures: w.failed.Load(), DroppedAttempts: w.droppedAttempts.Load()}
}
func (w *Worker) Drain(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.queue)
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		w.cancel()
		return nil
	case <-ctx.Done():
		w.cancel()
		return ctx.Err()
	}
}
func (w *Worker) Wait() { <-w.done }

var ErrInvalidQuery = errors.New("invalid usage query")
