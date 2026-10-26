// Package limits implements shared, per-key token reservations in Redis.
package limits

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/reserve.lua
var reserveLua string

//go:embed lua/reconcile.lua
var reconcileLua string

var ErrUnavailable = errors.New("shared token store unavailable")

type Denied struct {
	Remaining  int64
	RetryAfter time.Duration
}

func (d *Denied) Error() string { return "token budget exhausted" }

type Policy struct{ Capacity, RefillPerSecond int64 }

type Reservation struct {
	Policy            Policy
	Key, ID           string
	Amount, Remaining int64
}
type Store interface {
	Reserve(context.Context, string, string, int64, ...Policy) (Reservation, error)
	Reconcile(context.Context, Reservation, *int64) error
	Ping(context.Context) error
	Close() error
}
type Options struct {
	Address, Password, Prefix string
	DB                        int
	Capacity, RefillPerSecond int64
	Timeout, ReservationTTL   time.Duration
}
type Redis struct {
	client             *redis.Client
	options            Options
	reserve, reconcile *redis.Script
}

func New(o Options) (*Redis, error) {
	if o.Address == "" || o.Prefix == "" || o.Capacity < 1 || o.Capacity > 1_000_000_000 || o.RefillPerSecond < 1 || o.RefillPerSecond > 1_000_000 || o.Timeout <= 0 || o.ReservationTTL <= 0 || o.DB < 0 || o.DB > 15 {
		return nil, errors.New("invalid Redis limiter options")
	}
	client := redis.NewClient(&redis.Options{Addr: o.Address, Password: o.Password, DB: o.DB, DialTimeout: o.Timeout, ReadTimeout: o.Timeout, WriteTimeout: o.Timeout, PoolTimeout: o.Timeout, ContextTimeoutEnabled: true, MaxRetries: -1, PoolSize: 32, MaxActiveConns: 32})
	return &Redis{client: client, options: o, reserve: redis.NewScript(reserveLua), reconcile: redis.NewScript(reconcileLua)}, nil
}
func (s *Redis) keys(key, id string) []string {
	return []string{fmt.Sprintf("%s:{%s}:bucket", s.options.Prefix, key), fmt.Sprintf("%s:{%s}:reservation:%s", s.options.Prefix, key, id)}
}
func (s *Redis) Reserve(ctx context.Context, key, id string, amount int64, policies ...Policy) (Reservation, error) {
	policy := Policy{s.options.Capacity, s.options.RefillPerSecond}
	if len(policies) > 1 {
		return Reservation{}, errors.New("invalid policy")
	}
	if len(policies) == 1 {
		policy = policies[0]
	}
	r := Reservation{Key: key, ID: id, Amount: amount, Policy: policy}
	if policy.Capacity < 1 || policy.Capacity > 1_000_000_000 || policy.RefillPerSecond < 1 || policy.RefillPerSecond > 1_000_000 {
		return r, errors.New("invalid policy")
	}
	if amount < 1 || amount > 1_000_000_000 {
		return r, errors.New("invalid reservation amount")
	}
	ctx, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	v, err := s.reserve.Run(ctx, s.client, s.keys(key, id), policy.Capacity, policy.RefillPerSecond, amount, s.options.ReservationTTL.Milliseconds()).Int64Slice()
	if err != nil || len(v) != 3 {
		return r, ErrUnavailable
	}
	r.Remaining = v[1]
	switch v[0] {
	case 1:
		return r, nil
	case 0:
		return r, &Denied{Remaining: v[1], RetryAfter: time.Duration(v[2]) * time.Millisecond}
	default:
		return r, ErrUnavailable
	}
}

// A nil actual settles unknown usage without a refund. Missing/expired records
// are never recreated, and a second reconciliation cannot refund twice.
func (s *Redis) Reconcile(ctx context.Context, r Reservation, actual *int64) error {
	n := int64(-1)
	if actual != nil {
		n = *actual
		if n < 0 || n > 1_000_000_000 {
			return errors.New("invalid actual token count")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	policy := r.Policy
	if policy.Capacity == 0 {
		policy = Policy{s.options.Capacity, s.options.RefillPerSecond}
	}
	_, err := s.reconcile.Run(ctx, s.client, s.keys(r.Key, r.ID), n, policy.Capacity, policy.RefillPerSecond, s.options.ReservationTTL.Milliseconds()).Int64()
	if err != nil {
		return ErrUnavailable
	}
	return nil
}
func (s *Redis) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.options.Timeout)
	defer cancel()
	if s.client.Ping(ctx).Err() != nil {
		return ErrUnavailable
	}
	return nil
}
func (s *Redis) Close() error { return s.client.Close() }
