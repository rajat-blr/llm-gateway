package gateway

import (
	"context"
	"testing"
	"time"
)

func TestFlightsBoundWaitersAndSharedLifetime(t *testing.T) {
	group := newFlights(4)
	started := make(chan struct{})
	run := func(ctx context.Context, id string) inferenceResult {
		close(started)
		<-ctx.Done()
		return failureResult(ctx)
	}
	call, leader, err := group.join("one", time.Now().Add(time.Minute), run)
	if err != nil || !leader {
		t.Fatal("owner not admitted")
	}
	<-started
	for i := 0; i < 3; i++ {
		joined, leader, err := group.join("one", time.Now().Add(time.Minute), run)
		if err != nil || leader || joined != call {
			t.Fatal("follower did not join")
		}
	}
	if _, _, err := group.join("one", time.Now().Add(time.Minute), run); err == nil {
		t.Fatal("unbounded waiters admitted")
	}
	for i := 0; i < 4; i++ {
		group.leave()
	}
	if _, _, err := group.join("second", time.Now().Add(time.Minute), run); err == nil {
		t.Fatal("unbounded active executions admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if group.shutdown(ctx) != context.DeadlineExceeded {
		t.Fatal("shutdown ignored shared execution budget")
	}
	select {
	case <-call.done:
	case <-time.After(time.Second):
		t.Fatal("shared execution did not observe shutdown cancellation")
	}
	if _, _, err := group.join("one", time.Now().Add(time.Minute), run); err == nil {
		t.Fatal("work admitted after shutdown")
	}
}
