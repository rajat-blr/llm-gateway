package gateway

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errFlightAdmission = errors.New("shared execution admission unavailable")

type sharedCall struct {
	id     string
	done   chan struct{}
	result inferenceResult
}
type flightGroup struct {
	mu             sync.Mutex
	calls          map[string]*sharedCall
	waiters, limit int
	closing        bool
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
}

func newFlights(limit int) *flightGroup {
	ctx, cancel := context.WithCancel(context.Background())
	return &flightGroup{calls: map[string]*sharedCall{}, limit: limit, ctx: ctx, cancel: cancel, done: make(chan struct{})}
}
func (g *flightGroup) join(key string, deadline time.Time, run func(context.Context, string) inferenceResult) (*sharedCall, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing || g.waiters >= g.limit || (g.calls[key] == nil && len(g.calls) >= max(1, g.limit/4)) {
		return nil, false, errFlightAdmission
	}
	g.waiters++
	if call := g.calls[key]; call != nil {
		return call, false, nil
	}
	call := &sharedCall{id: "exec-" + requestID(), done: make(chan struct{})}
	g.calls[key] = call
	ctx, cancel := context.WithDeadline(g.ctx, deadline)
	go func() {
		defer cancel()
		result := run(ctx, call.id)
		g.mu.Lock()
		defer g.mu.Unlock()
		call.result = result
		delete(g.calls, key)
		close(call.done)
		if g.closing && len(g.calls) == 0 {
			close(g.done)
		}
	}()
	return call, true, nil
}
func (g *flightGroup) leave() { g.mu.Lock(); g.waiters--; g.mu.Unlock() }
func (g *flightGroup) stats() (int, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls), g.waiters
}
func (g *flightGroup) shutdown(ctx context.Context) error {
	g.mu.Lock()
	if !g.closing {
		g.closing = true
		if len(g.calls) == 0 {
			close(g.done)
		}
	}
	g.mu.Unlock()
	select {
	case <-g.done:
		g.cancel()
		return nil
	case <-ctx.Done():
		g.cancel()
		return ctx.Err()
	}
}
