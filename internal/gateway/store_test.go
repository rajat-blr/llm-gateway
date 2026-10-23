package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rajat-blr/llm-gateway/internal/limits"
)

// The protocol suite isolates HTTP behavior from Redis. Shared accounting is
// tested separately against real Redis, including two independent clients.
type memoryStore struct {
	mu                sync.Mutex
	capacity, balance int64
	pending           map[string]int64
	reservations      int
	reconciliations   int
	actual            []int64
	down              bool
	failReconcile     bool
}

func newMemoryStore() *memoryStore {
	return &memoryStore{capacity: 1_000_000, balance: 1_000_000, pending: map[string]int64{}}
}
func (s *memoryStore) Reserve(ctx context.Context, key, id string, amount int64) (limits.Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := limits.Reservation{Key: key, ID: id, Amount: amount}
	if s.down {
		return r, limits.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	if s.balance < amount {
		return r, &limits.Denied{Remaining: s.balance}
	}
	s.balance -= amount
	r.Remaining = s.balance
	s.pending[id] = amount
	s.reservations++
	return r, nil
}
func (s *memoryStore) Reconcile(ctx context.Context, r limits.Reservation, actual *int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down || s.failReconcile {
		return limits.ErrUnavailable
	}
	amount, ok := s.pending[r.ID]
	if !ok {
		return nil
	}
	delete(s.pending, r.ID)
	s.reconciliations++
	if actual != nil {
		s.balance = min(s.capacity, s.balance+amount-*actual)
		s.actual = append(s.actual, *actual)
	} else {
		s.actual = append(s.actual, -1)
	}
	return nil
}
func (s *memoryStore) Ping(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return errors.New("test store down")
	}
	return nil
}
func (s *memoryStore) Close() error { return nil }
func unitServer(t *testing.T, c Config) (*Server, error) {
	t.Helper()
	routes := map[string]Route{}
	for alias, route := range c.Routes {
		route.Targets = append([]Target(nil), route.Targets...)
		for i := range route.Targets {
			if route.Targets[i].AttemptTimeout > c.RequestTimeout {
				route.Targets[i].AttemptTimeout = c.RequestTimeout
			}
		}
		routes[alias] = route
	}
	c.Routes = routes
	if c.BackoffMax > c.RequestTimeout {
		c.BackoffMax = c.RequestTimeout
	}
	return newServer(c, newMemoryStore())
}
