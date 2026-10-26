// Package reuse provides bounded exact-response storage. Cache failures are
// independent of mandatory token admission and may be treated as misses.
package reuse

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrCache = errors.New("cache unavailable or invalid entry")

type Store interface {
	Get(context.Context, string, int) ([]byte, bool, error)
	Put(context.Context, string, []byte, time.Duration) error
	Close() error
}
type Redis struct {
	client  *redis.Client
	prefix  string
	timeout time.Duration
}

func New(address, password, prefix string, db int, timeout time.Duration) *Redis {
	client := redis.NewClient(&redis.Options{Addr: address, Password: password, DB: db, DialTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout, PoolTimeout: timeout, ContextTimeoutEnabled: true, MaxRetries: -1, PoolSize: 16, MaxActiveConns: 16})
	return &Redis{client: client, prefix: prefix + ":cache:", timeout: timeout}
}
func (r *Redis) Get(ctx context.Context, key string, maxBytes int) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	// The inclusive endpoint returns at most maxBytes+1, avoiding a large GET
	// allocation even when an external writer corrupts an entry's size.
	data, err := r.client.GetRange(ctx, r.prefix+key, 0, int64(maxBytes)).Bytes()
	if err != nil || len(data) > maxBytes {
		return nil, false, ErrCache
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	return data, true, nil
}
func (r *Redis) Put(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	if r.client.Set(ctx, r.prefix+key, data, ttl).Err() != nil {
		return ErrCache
	}
	return nil
}
func (r *Redis) Close() error { return r.client.Close() }
