package limits

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func stores(t *testing.T, capacity, refill int64) (*Redis, *Redis) {
	t.Helper()
	address := os.Getenv("REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("set REDIS_TEST_ADDR for real Redis acceptance tests")
	}
	options := Options{Address: address, Prefix: fmt.Sprintf("test:%d", time.Now().UnixNano()), Capacity: capacity, RefillPerSecond: refill, Timeout: time.Second, ReservationTTL: time.Minute}
	a, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, err := a.client.Scan(ctx, cursor, options.Prefix+":*", 100).Result()
			if err != nil {
				break
			}
			if len(keys) > 0 {
				a.client.Del(ctx, keys...)
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		a.Close()
		b.Close()
	})
	return a, b
}
func TestTwoInstancesCannotOverReserve(t *testing.T) {
	a, b := stores(t, 1000, 1)
	var admitted atomic.Int64
	var failures atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 0 {
				store = b
			}
			_, err := store.Reserve(context.Background(), "key", fmt.Sprint(i), 100)
			if err == nil {
				admitted.Add(1)
			} else if _, ok := err.(*Denied); !ok {
				failures.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 10 || failures.Load() != 0 {
		t.Fatalf("admitted=%d store errors=%d", admitted.Load(), failures.Load())
	}
}
func TestReconcileIsIdempotent(t *testing.T) {
	a, b := stores(t, 1000, 1)
	ctx := context.Background()
	r, err := a.Reserve(ctx, "key", "first", 900)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve(ctx, "key", "second", 100); err != nil {
		t.Fatal(err)
	}
	actual := int64(800)
	var wg sync.WaitGroup
	var errors atomic.Int64
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Reconcile(ctx, r, &actual) != nil {
				errors.Add(1)
			}
		}()
	}
	wg.Wait()
	if errors.Load() != 0 {
		t.Fatal("reconcile failed")
	}
	if _, err := a.Reserve(ctx, "key", "third", 200); err == nil {
		t.Fatal("duplicate refund credited twice")
	}
	if _, err := b.Reserve(ctx, "key", "fourth", 100); err != nil {
		t.Fatal("refund was lost")
	}
}
func TestUnknownDebitNegativeBalanceAndExpiredRecord(t *testing.T) {
	a, b := stores(t, 1000, 1)
	ctx := context.Background()
	r, err := a.Reserve(ctx, "key", "unknown", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Reconcile(ctx, r, nil); err != nil {
		t.Fatal(err)
	}
	zero := int64(0)
	b.Reconcile(ctx, r, &zero)
	if _, err := a.Reserve(ctx, "key", "again", 100); err == nil {
		t.Fatal("unknown debit refunded")
	}
	r, err = a.Reserve(ctx, "other", "debt", 1000)
	if err != nil {
		t.Fatal(err)
	}
	actual := int64(2000)
	if err := b.Reconcile(ctx, r, &actual); err != nil {
		t.Fatal(err)
	}
	bucket := a.keys("other", "debt")[0]
	tokens, err := a.client.HGet(ctx, bucket, "tokens").Int64()
	if err != nil || tokens > -900_000 {
		t.Fatalf("debt %d: %v", tokens, err)
	}
	ttl := a.client.PTTL(ctx, bucket).Val()
	if ttl < 1900*time.Second {
		t.Fatalf("bucket can expire before debt refill: %v", ttl)
	}
	r, err = a.Reserve(ctx, "expired", "request", 900)
	if err != nil {
		t.Fatal(err)
	}
	a.client.PExpire(ctx, a.keys("expired", "request")[1], time.Millisecond)
	expiryDeadline := time.Now().Add(time.Second)
	for a.client.Exists(ctx, a.keys("expired", "request")[1]).Val() != 0 {
		if time.Now().After(expiryDeadline) {
			t.Fatal("reservation TTL did not expire")
		}
		time.Sleep(time.Millisecond)
	}
	if err := b.Reconcile(ctx, r, &zero); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reserve(ctx, "expired", "fresh", 200); err == nil {
		t.Fatal("expired reservation recreated credit")
	}
}
func TestServerTimeRefillAndReservationRetry(t *testing.T) {
	a, _ := stores(t, 100, 10)
	ctx := context.Background()
	r, err := a.Reserve(ctx, "key", "request", 100)
	if err != nil {
		t.Fatal(err)
	}
	again, err := a.Reserve(ctx, "key", "request", 100)
	if err != nil || again.Remaining != r.Remaining {
		t.Fatal("reserve replay debited twice")
	}
	bucket := a.keys("key", "request")[0]
	now := a.client.Time(ctx).Val().UnixMilli()
	a.client.HSet(ctx, bucket, "time", now-5000)
	if _, err := a.Reserve(ctx, "key", "refilled", 50); err != nil {
		t.Fatal("server-time refill failed")
	}
	if _, err := a.Reserve(ctx, "key", "too-large", 101); err == nil {
		t.Fatal("admitted above capacity")
	}
	// Same identity with incompatible deployment policies must fail closed.
	a.options.Capacity = 200
	if _, err := a.Reserve(ctx, "key", "policy-mismatch", 1); err != ErrUnavailable {
		t.Fatalf("policy mismatch: %v", err)
	}
}
func TestRedisOutageFailsClosed(t *testing.T) {
	a, err := New(Options{Address: "127.0.0.1:1", Prefix: "outage", Capacity: 100, RefillPerSecond: 1, Timeout: 50 * time.Millisecond, ReservationTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.Reserve(context.Background(), "key", "id", 1); err != ErrUnavailable {
		t.Fatalf("outage: %v", err)
	}
}
