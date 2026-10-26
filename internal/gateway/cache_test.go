package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rajat-blr/llm-gateway/internal/reuse"
	"github.com/rajat-blr/llm-gateway/internal/storage"
)

type memoryCacheEntry struct {
	data  []byte
	until time.Time
}
type memoryCache struct {
	mu                  sync.Mutex
	entries             map[string]memoryCacheEntry
	gets, puts          int
	readFail, writeFail bool
	corrupt             []byte
}

func newMemoryCache() *memoryCache { return &memoryCache{entries: map[string]memoryCacheEntry{}} }
func (m *memoryCache) Get(ctx context.Context, key string, max int) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	if m.readFail {
		return nil, false, reuse.ErrCache
	}
	if m.corrupt != nil {
		return append([]byte(nil), m.corrupt...), true, nil
	}
	e, ok := m.entries[key]
	if !ok || time.Now().After(e.until) {
		return nil, false, nil
	}
	if len(e.data) > max {
		return nil, false, reuse.ErrCache
	}
	return append([]byte(nil), e.data...), true, nil
}
func (m *memoryCache) Put(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeFail {
		return reuse.ErrCache
	}
	m.puts++
	m.entries[key] = memoryCacheEntry{append([]byte(nil), data...), time.Now().Add(ttl)}
	return nil
}
func (m *memoryCache) Close() error { return nil }

type cacheFixture struct {
	url, secret string
	key         storage.Key
	server      *Server
	cache       *memoryCache
	store       *memoryStore
	calls       atomic.Int64
}

func cacheHarness(t *testing.T, handler http.Handler, change func(*Config)) *cacheFixture {
	t.Helper()
	f := &cacheFixture{cache: newMemoryCache(), store: newMemoryStore()}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.calls.Add(1); handler.ServeHTTP(w, r) }))
	t.Cleanup(upstream.Close)
	c := testConfig(t, upstream.URL, "openai")
	route := c.Routes["chat"]
	route.Cache = CachePolicy{Enabled: true, TTL: Duration(time.Minute), MaxEntryBytes: 65536}
	c.Routes["chat"] = route
	if change != nil {
		change(&c)
	}
	server, err := newServer(c, f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.server = server
	server.cache.Close()
	server.cache = f.cache
	f.key, f.secret, err = server.identities.CreateKey(context.Background(), storage.KeySpec{Name: "reuse", Aliases: []string{"chat"}, Capacity: 10000, RefillPerSecond: 1, MaxOutputTokens: 100, AllowReuse: true})
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(server)
	f.url = gateway.URL
	t.Cleanup(func() { gateway.Close(); server.Close() })
	return f
}
func cachedCall(t *testing.T, f *cacheFixture, payload string) *http.Response {
	return authorized(t, f.url, "POST", "/v1/chat/completions", f.secret, payload)
}
func TestExactCacheScopeMetadataAndAccounting(t *testing.T) {
	f := cacheHarness(t, successfulFixture(), nil)
	first := cachedCall(t, f, input)
	b := body(t, first)
	if first.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("cold cache: %s", b)
	}
	second := cachedCall(t, f, ` { "messages" : [{"content":"hello","role":"user"}], "model":"chat" } `)
	secondBody := body(t, second)
	if second.Header.Get("X-Cache") != "HIT" || first.Header.Get("X-Request-ID") == second.Header.Get("X-Request-ID") || !strings.Contains(secondBody, second.Header.Get("X-Request-ID")) {
		t.Fatal("cache reused client metadata or missed canonical JSON")
	}
	if f.calls.Load() != 1 {
		t.Fatal("hit called upstream")
	}
	other, secret, err := f.server.identities.CreateKey(context.Background(), f.key.KeySpec)
	if err != nil {
		t.Fatal(err)
	}
	response := authorized(t, f.url, "POST", "/v1/chat/completions", secret, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "MISS" || other.ID == f.key.ID || f.calls.Load() != 2 {
		t.Fatal("cache crossed gateway keys")
	}
	response = call(t, f.url, input)
	body(t, response)
	response = call(t, f.url, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "BYPASS" || f.calls.Load() != 4 {
		t.Fatal("key without opt-in reused work")
	}
	eventually(t, func() bool { return f.server.usage.Stats().Written == 7 })
	report, err := f.server.identities.Usage(context.Background(), storage.UsageQuery{FromMS: time.Now().Add(-time.Hour).UnixMilli(), ToMS: time.Now().Add(time.Second).UnixMilli(), KeyID: f.key.ID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var requests, attempts, hits, clientOutput, providerOutput int64
	for _, r := range report.Requests {
		requests += r.Requests
		hits += r.CacheHits
		clientOutput += r.ClientOutput
	}
	for _, a := range report.Attempts {
		attempts += a.Attempts
		providerOutput += a.Output
	}
	if requests != 2 || attempts != 1 || hits != 1 || clientOutput != 6 || providerOutput != 3 {
		t.Fatalf("reuse double-counted attempts: %+v", report)
	}
	f.store.mu.Lock()
	balance := f.store.balance
	f.store.mu.Unlock()
	if balance != 1_000_000-32 {
		t.Fatalf("hit did not refund admission: %d", balance)
	}
}
func TestCacheDistinguishesEffectiveInputsAndOmissions(t *testing.T) {
	f := cacheHarness(t, successfulFixture(), nil)
	variants := []string{input, strings.TrimSuffix(input, "}") + `,"temperature":0}`, strings.TrimSuffix(input, "}") + `,"max_tokens":100}`, strings.Replace(input, "hello", " hello ", 1), strings.TrimSuffix(input, "}") + `,"stop":"end"}`, strings.TrimSuffix(input, "}") + `,"stop":["end"]}`}
	for _, payload := range variants {
		response := cachedCall(t, f, payload)
		body(t, response)
		if response.Header.Get("X-Cache") != "MISS" {
			t.Fatalf("distinct input reused: %s", payload)
		}
	}
	if f.calls.Load() != int64(len(variants)) {
		t.Fatal("generation inputs collided")
	}
	response := cachedCall(t, f, streamInput)
	b := body(t, response)
	if response.Header.Get("X-Cache") != "BYPASS" || !strings.Contains(b, "[DONE]") {
		t.Fatal("stream entered reuse path")
	}
}
func heldFixture(started chan struct{}, release chan struct{}, canceled chan struct{}) http.Handler {
	base := successfulFixture()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		close(started)
		select {
		case <-release:
			r.Body = io.NopCloser(bytes.NewReader(data))
			base.ServeHTTP(w, r)
		case <-r.Context().Done():
			if canceled != nil {
				close(canceled)
			}
		}
	})
}

type asyncReply struct {
	status      int
	cache, body string
	err         error
}

func asyncCached(ctx context.Context, f *cacheFixture) <-chan asyncReply {
	done := make(chan asyncReply, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, "POST", f.url+"/v1/chat/completions", strings.NewReader(input))
		req.Header.Set("Authorization", "Bearer "+f.secret)
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- asyncReply{err: err}
			return
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		done <- asyncReply{status: response.StatusCode, cache: response.Header.Get("X-Cache"), body: string(data), err: err}
	}()
	return done
}
func TestCoalescingOwnerAndFollowerCancelIndependently(t *testing.T) {
	started, release, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f := cacheHarness(t, heldFixture(started, release, canceled), nil)
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := asyncCached(ownerCtx, f)
	<-started
	follower := asyncCached(context.Background(), f)
	cancelCtx, cancelFollower := context.WithCancel(context.Background())
	leaver := asyncCached(cancelCtx, f)
	eventually(t, func() bool { return f.server.reuseCounters.coalesced.Load() == 2 })
	cancelOwner()
	cancelFollower()
	if (<-owner).err == nil || (<-leaver).err == nil {
		t.Fatal("canceled callers did not stop waiting")
	}
	select {
	case <-canceled:
		t.Fatal("initiator canceled shared execution")
	default:
	}
	// Release explicitly, leaving the deferred closure guarded below.
	close(release)
	response := <-follower
	if response.err != nil || response.status != 200 || response.cache != "COALESCED" {
		t.Fatalf("surviving follower: %+v", response)
	}
	if f.calls.Load() != 1 {
		t.Fatal("overlapping requests executed more than once")
	}
	eventually(t, func() bool { return f.server.usage.Stats().Written == 4 })
	report, err := f.server.identities.Usage(context.Background(), storage.UsageQuery{FromMS: time.Now().Add(-time.Hour).UnixMilli(), ToMS: time.Now().Add(time.Second).UnixMilli(), KeyID: f.key.ID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var clients, attempts int64
	for _, r := range report.Requests {
		clients += r.Requests
	}
	for _, a := range report.Attempts {
		attempts += a.Attempts
	}
	if clients != 3 || attempts != 1 {
		t.Fatal("shared ownership mixed client and upstream records")
	}
	f.store.mu.Lock()
	balance := f.store.balance
	f.store.mu.Unlock()
	if balance != 1_000_000-8 {
		t.Fatal("followers consumed upstream token budget")
	}
}
func TestSharedExecutionCanFinishAfterAllCallersLeave(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f := cacheHarness(t, heldFixture(started, release, nil), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := asyncCached(ctx, f)
	<-started
	cancel()
	<-done
	eventually(t, func() bool { active, waiters := f.server.flights.stats(); return active == 1 && waiters == 0 })
	close(release)
	eventually(t, func() bool { f.cache.mu.Lock(); defer f.cache.mu.Unlock(); return f.cache.puts == 1 })
	response := cachedCall(t, f, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "HIT" || f.calls.Load() != 1 {
		t.Fatal("bounded abandoned execution did not populate cache")
	}
}
func TestCacheFailuresAreMissesAndAdmissionStillFailsClosed(t *testing.T) {
	f := cacheHarness(t, successfulFixture(), nil)
	f.cache.readFail = true
	f.cache.writeFail = true
	for i := 0; i < 2; i++ {
		response := cachedCall(t, f, input)
		body(t, response)
		if response.StatusCode != 200 {
			t.Fatal("cache outage blocked admitted inference")
		}
	}
	if f.calls.Load() != 2 || f.server.reuseCounters.readErrors.Load() != 2 || f.server.reuseCounters.writeErrors.Load() != 2 {
		t.Fatal("cache failure semantics missing")
	}
	f.cache.mu.Lock()
	f.cache.readFail = false
	f.cache.writeFail = false
	f.cache.mu.Unlock()
	response := cachedCall(t, f, input)
	body(t, response)
	f.store.mu.Lock()
	f.store.down = true
	f.store.mu.Unlock()
	f.cache.mu.Lock()
	gets := f.cache.gets
	f.cache.mu.Unlock()
	response = cachedCall(t, f, input)
	body(t, response)
	f.cache.mu.Lock()
	after := f.cache.gets
	f.cache.mu.Unlock()
	if response.StatusCode != 503 || gets != after {
		t.Fatal("cache hit bypassed required shared admission")
	}
}
func TestCorruptAndOversizedEntriesAreNotReplayed(t *testing.T) {
	f := cacheHarness(t, successfulFixture(), nil)
	f.cache.corrupt = []byte(`{"schema":1,"value":{"message":{"role":"tool","content":"bad"}},"usage_source":"unknown"}`)
	response := cachedCall(t, f, input)
	body(t, response)
	if response.StatusCode != 200 || response.Header.Get("X-Cache") != "MISS" {
		t.Fatal("corrupt entry replayed")
	}
	f = cacheHarness(t, successfulFixture(), func(c *Config) { r := c.Routes["chat"]; r.Cache.MaxEntryBytes = 128; c.Routes["chat"] = r })
	for i := 0; i < 2; i++ {
		response = cachedCall(t, f, input)
		body(t, response)
	}
	if f.calls.Load() != 2 || f.server.reuseCounters.oversized.Load() != 2 {
		t.Fatal("oversized successful entry was cached")
	}
}
func TestCacheExpiryAndRevisionInvalidation(t *testing.T) {
	f := cacheHarness(t, successfulFixture(), nil)
	response := cachedCall(t, f, input)
	body(t, response)
	f.cache.mu.Lock()
	for key, entry := range f.cache.entries {
		entry.until = time.Now().Add(-time.Second)
		f.cache.entries[key] = entry
	}
	f.cache.mu.Unlock()
	response = cachedCall(t, f, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "MISS" {
		t.Fatal("expired entry reused")
	}
	candidate := copyConfig(t, f.server.routing.Load().config)
	r := candidate.Routes["chat"]
	r.Revision++
	candidate.Routes["chat"] = r
	if err := f.server.publish(candidate); err != nil {
		t.Fatal(err)
	}
	response = cachedCall(t, f, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "MISS" || f.calls.Load() != 3 {
		t.Fatal("route revision did not invalidate cache")
	}
}
func copyConfig(t *testing.T, c Config) Config {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var clone Config
	if err = StrictJSON(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func TestErrorsAreNotCachedAndRouteOptOutBypassesReuse(t *testing.T) {
	f := cacheHarness(t, failStatus(503), nil)
	for i := 0; i < 2; i++ {
		response := cachedCall(t, f, input)
		body(t, response)
		if response.StatusCode != 502 {
			t.Fatal("provider failure became successful reuse")
		}
	}
	f.cache.mu.Lock()
	puts := f.cache.puts
	f.cache.mu.Unlock()
	if puts != 0 || f.calls.Load() != 2 {
		t.Fatal("error was cached")
	}
	f = cacheHarness(t, successfulFixture(), func(c *Config) { r := c.Routes["chat"]; r.Cache = CachePolicy{}; c.Routes["chat"] = r })
	for i := 0; i < 2; i++ {
		response := cachedCall(t, f, input)
		body(t, response)
		if response.Header.Get("X-Cache") != "BYPASS" {
			t.Fatal("disabled route reused a completion")
		}
	}
	f.cache.mu.Lock()
	gets := f.cache.gets
	f.cache.mu.Unlock()
	if gets != 0 || f.calls.Load() != 2 {
		t.Fatal("route opt-out accessed cache")
	}
}
func TestReusableFallbackSettlesAttemptsAndUnusedAdmission(t *testing.T) {
	var secondaryCalls atomic.Int64
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryCalls.Add(1)
		successfulFixture().ServeHTTP(w, r)
	}))
	defer secondary.Close()
	f := cacheHarness(t, failStatus(503), func(c *Config) {
		c.Breaker.Failures = 1
		c.Providers["backup"] = ProviderConfig{Kind: "openai", BaseURL: secondary.URL + "/v1", KeyEnv: "TEST_PROVIDER_KEY"}
		r := c.Routes["chat"]
		r.Targets = append(r.Targets, Target{Provider: "backup", Model: "backup", AttemptTimeout: c.RequestTimeout})
		c.Routes["chat"] = r
	})
	first := cachedCall(t, f, input)
	body(t, first)
	if first.StatusCode != 200 || first.Header.Get("X-Gateway-Provider") != "backup" {
		t.Fatal("reusable fallback failed")
	}
	response := cachedCall(t, f, strings.Replace(input, "hello", "second", 1))
	body(t, response)
	if f.calls.Load() != 1 || secondaryCalls.Load() != 2 {
		t.Fatal("open primary was not skipped")
	}
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if f.store.reservations != 4 || len(f.store.actual) != 4 || f.store.actual[0] != -1 || f.store.actual[1] != 8 || f.store.actual[2] != 0 || f.store.actual[3] != 8 {
		t.Fatalf("cache admission/attempt ownership incorrect: %+v", f.store.actual)
	}
}
func TestTwoGatewaysShareExactRedisCache(t *testing.T) {
	address := os.Getenv("REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("set REDIS_TEST_ADDR for shared cache acceptance")
	}
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); successfulFixture().ServeHTTP(w, r) }))
	defer upstream.Close()
	c := testConfig(t, upstream.URL, "openai")
	c.Redis.Address = address
	c.Redis.Prefix = fmt.Sprintf("gateway-reuse:%d", time.Now().UnixNano())
	r := c.Routes["chat"]
	r.Cache = CachePolicy{Enabled: true, TTL: Duration(time.Minute), MaxEntryBytes: 65536}
	c.Routes["chat"] = r
	a, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	first := httptest.NewServer(a)
	defer first.Close()
	_, secret, err := a.identities.CreateKey(context.Background(), storage.KeySpec{Name: "shared", Aliases: []string{"chat"}, Capacity: 1000, RefillPerSecond: 1, MaxOutputTokens: 100, AllowReuse: true})
	if err != nil {
		t.Fatal(err)
	}
	response := authorized(t, first.URL, "POST", "/v1/chat/completions", secret, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "MISS" {
		t.Fatal("first gateway did not execute")
	}
	eventually(t, func() bool { return a.usage.Stats().Written == 2 })
	b, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	second := httptest.NewServer(b)
	defer second.Close()
	response = authorized(t, second.URL, "POST", "/v1/chat/completions", secret, input)
	body(t, response)
	if response.Header.Get("X-Cache") != "HIT" || calls.Load() != 1 {
		t.Fatal("sequential requests across gateways did not share the exact cache")
	}
}
