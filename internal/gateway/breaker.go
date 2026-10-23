package gateway

import (
	"sync"
	"time"
)

type BreakerConfig struct {
	Failures          int      `json:"failures"`
	Window            Duration `json:"window"`
	OpenFor           Duration `json:"open_for"`
	RecoverySuccesses int      `json:"recovery_successes"`
}
type breakerResult uint8

const (
	breakerNeutral breakerResult = iota
	breakerSuccess
	breakerFailure
)

type breakerTicket struct {
	generation uint64
	probe      bool
}
type breaker struct {
	mu         sync.Mutex
	config     BreakerConfig
	now        func() time.Time
	generation uint64
	state      string
	failures   []time.Time
	until      time.Time
	probe      bool
	recovered  int
}

func newBreaker(c BreakerConfig, now func() time.Time) *breaker {
	return &breaker{config: c, now: now, state: "closed"}
}
func (b *breaker) acquire() (breakerTicket, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == "open" {
		if b.now().Before(b.until) {
			return breakerTicket{}, false
		}
		b.state = "half_open"
		b.probe = false
		b.recovered = 0
	}
	if b.state == "half_open" {
		if b.probe {
			return breakerTicket{}, false
		}
		b.probe = true
		return breakerTicket{b.generation, true}, true
	}
	return breakerTicket{generation: b.generation}, true
}
func (b *breaker) complete(ticket breakerTicket, result breakerResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ticket.generation != b.generation {
		return
	}
	now := b.now()
	open := func() {
		b.state = "open"
		b.until = now.Add(time.Duration(b.config.OpenFor))
		b.generation++
		b.probe = false
		b.failures = nil
		b.recovered = 0
	}
	if ticket.probe {
		b.probe = false
		switch result {
		case breakerFailure:
			open()
		case breakerSuccess:
			b.recovered++
			if b.recovered >= b.config.RecoverySuccesses {
				b.state = "closed"
				b.generation++
				b.failures = nil
			}
		}
		return
	}
	if result != breakerFailure || b.state != "closed" {
		return
	}
	cutoff := now.Add(-time.Duration(b.config.Window))
	first := 0
	for first < len(b.failures) && !b.failures[first].After(cutoff) {
		first++
	}
	b.failures = append(b.failures[:0], b.failures[first:]...)
	b.failures = append(b.failures, now)
	if len(b.failures) >= b.config.Failures {
		open()
	}
}
