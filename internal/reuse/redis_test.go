package reuse

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRedisEntryBoundsTTLAndNamespace(t *testing.T) {
	address := os.Getenv("REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("set REDIS_TEST_ADDR for cache acceptance")
	}
	prefix := fmt.Sprintf("cache-test:%d", time.Now().UnixNano())
	a := New(address, "", prefix, 0, time.Second)
	b := New(address, "", prefix, 0, time.Second)
	defer a.Close()
	defer b.Close()
	ctx := context.Background()
	key := "exact-entry"
	if err := a.Put(ctx, key, []byte("cached completion"), time.Second); err != nil {
		t.Fatal(err)
	}
	data, hit, err := b.Get(ctx, key, 100)
	if err != nil || !hit || string(data) != "cached completion" {
		t.Fatal("two clients did not share cached data")
	}
	if data, hit, err = b.Get(ctx, key, 4); err == nil || hit || len(data) != 0 {
		t.Fatal("oversized external entry bypassed read bound")
	}
	ttl := a.client.PTTL(ctx, a.prefix+key).Val()
	if ttl <= 0 || ttl > time.Second {
		t.Fatal("configured cache TTL missing")
	}
	a.client.PExpire(ctx, a.prefix+key, time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, hit, err = b.Get(ctx, key, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !hit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cache TTL did not expire")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if a.client.Exists(ctx, prefix+":{exact-entry}:bucket").Val() != 0 {
		t.Fatal("cache polluted limiter namespace")
	}
}
