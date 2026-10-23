package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBreakerClockAndProbeBudget(t *testing.T) {
	now := time.Unix(100, 0)
	b := newBreaker(BreakerConfig{Failures: 2, Window: Duration(time.Minute), OpenFor: Duration(10 * time.Second), RecoverySuccesses: 2}, func() time.Time { return now })
	fail := func() {
		ticket, ok := b.acquire()
		if !ok {
			t.Fatal("unexpected rejection")
		}
		b.complete(ticket, breakerFailure)
	}
	fail()
	now = now.Add(time.Minute)
	fail() // first failure aged out
	if _, ok := b.acquire(); !ok {
		t.Fatal("window did not expire")
	}
	stale, _ := b.acquire()
	fail()
	if _, ok := b.acquire(); ok {
		t.Fatal("breaker did not open")
	}
	b.complete(stale, breakerSuccess)
	if _, ok := b.acquire(); ok {
		t.Fatal("stale completion closed breaker")
	}
	now = now.Add(10 * time.Second)
	probe, ok := b.acquire()
	if !ok {
		t.Fatal("probe denied")
	}
	if _, ok := b.acquire(); ok {
		t.Fatal("concurrent probe admitted")
	}
	b.complete(probe, breakerNeutral)
	probe, ok = b.acquire()
	if !ok {
		t.Fatal("neutral outcome leaked probe")
	}
	b.complete(probe, breakerSuccess)
	probe, ok = b.acquire()
	if !ok {
		t.Fatal("second recovery denied")
	}
	b.complete(probe, breakerSuccess)
	if b.state != "closed" {
		t.Fatal("did not recover")
	}
	fail()
	fail()
	now = now.Add(10 * time.Second)
	probe, _ = b.acquire()
	b.complete(probe, breakerFailure)
	if _, ok := b.acquire(); ok {
		t.Fatal("failed probe did not reopen")
	}
}
func TestBreakerConcurrentHalfOpen(t *testing.T) {
	now := time.Unix(100, 0)
	b := newBreaker(BreakerConfig{Failures: 1, Window: Duration(time.Minute), OpenFor: Duration(time.Second), RecoverySuccesses: 1}, func() time.Time { return now })
	ticket, _ := b.acquire()
	b.complete(ticket, breakerFailure)
	now = now.Add(time.Second)
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := b.acquire(); ok {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("admitted %d probes", admitted.Load())
	}
}
