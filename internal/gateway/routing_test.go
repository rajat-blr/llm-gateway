package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mockprovider "github.com/rajat-blr/llm-gateway/internal/mock"
)

type routingFixture struct {
	url                string
	server             *Server
	store              *memoryStore
	primary, secondary atomic.Int64
}

func newRoutingFixture(t *testing.T, first, second http.Handler, change func(*Config, *memoryStore)) *routingFixture {
	t.Helper()
	fixture := &routingFixture{store: newMemoryStore()}
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fixture.primary.Add(1); first.ServeHTTP(w, r) }))
	t.Cleanup(primary.Close)
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fixture.secondary.Add(1); second.ServeHTTP(w, r) }))
	t.Cleanup(secondary.Close)
	cfg := testConfig(t, primary.URL, "openai")
	cfg.Providers["secondary"] = ProviderConfig{Kind: "openai", BaseURL: secondary.URL + "/v1", KeyEnv: "TEST_PROVIDER_KEY"}
	route := cfg.Routes["chat"]
	route.Targets = append(route.Targets, Target{Provider: "secondary", Model: "secondary-model", AttemptTimeout: cfg.RequestTimeout})
	cfg.Routes["chat"] = route
	cfg.Breaker.Failures = 1
	if change != nil {
		change(&cfg, fixture.store)
	}
	server, err := newServer(cfg, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	fixture.server = server
	gateway := httptest.NewServer(server)
	fixture.url = gateway.URL
	t.Cleanup(func() { gateway.Close(); server.Close() })
	return fixture
}
func failStatus(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, "private provider error body")
	})
}
func successfulFixture() http.Handler {
	return mockprovider.New(mockprovider.Options{Chunks: 3, Payload: "hi "})
}
func TestFallbackReservesEveryAttempt(t *testing.T) {
	fixture := newRoutingFixture(t, failStatus(503), successfulFixture(), nil)
	response := call(t, fixture.url, input)
	b := body(t, response)
	if response.StatusCode != 200 || response.Header.Get("X-Gateway-Provider") != "secondary" || !strings.Contains(b, "hi hi hi ") {
		t.Fatalf("%d: %s", response.StatusCode, b)
	}
	fixture.store.mu.Lock()
	defer fixture.store.mu.Unlock()
	if fixture.primary.Load() != 1 || fixture.secondary.Load() != 1 || fixture.store.reservations != 2 || fixture.store.reconciliations != 2 || fixture.store.actual[0] != -1 || fixture.store.actual[1] != 8 || fixture.store.balance != fixture.store.capacity-108-8 {
		t.Fatalf("incorrect per-attempt accounting: %+v", fixture.store.actual)
	}
	if response.Header.Get("X-RateLimit-Remaining-Tokens") == "" {
		t.Fatal("missing admission balance")
	}
}
func TestFallbackEligibilityAndAttemptCap(t *testing.T) {
	for _, status := range []int{400, 401, 403, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fixture := newRoutingFixture(t, failStatus(status), failStatus(503), nil)
			response := call(t, fixture.url, input)
			body(t, response)
			eligible := status == 429 || status >= 500
			if fixture.primary.Load() != 1 || (fixture.secondary.Load() == 1) != eligible || fixture.store.reservations > 2 {
				t.Fatalf("status=%d first=%d second=%d", status, fixture.primary.Load(), fixture.secondary.Load())
			}
			if !eligible {
				for _, breaker := range fixture.server.routing.Load().breakers {
					if breaker.state != "closed" {
						t.Fatal("nontransient failure opened breaker")
					}
				}
			}
		})
	}
}
func TestRetryAfterOutsideDeadlineDoesNotExecuteSecondary(t *testing.T) {
	first := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(429)
	})
	fixture := newRoutingFixture(t, first, successfulFixture(), nil)
	response := call(t, fixture.url, input)
	response.Body.Close()
	if fixture.secondary.Load() != 0 {
		t.Fatal("retried before Retry-After")
	}
}
func TestAttemptTimeoutFallsBackWithinOverallBudget(t *testing.T) {
	first := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() })
	fixture := newRoutingFixture(t, first, successfulFixture(), func(c *Config, s *memoryStore) {
		c.Routes["chat"].Targets[0].AttemptTimeout = Duration(20 * time.Millisecond)
	})
	start := time.Now()
	response := call(t, fixture.url, input)
	body(t, response)
	if response.StatusCode != 200 || fixture.secondary.Load() != 1 || time.Since(start) > time.Second {
		t.Fatal("attempt deadline did not leave a bounded fallback budget")
	}
}
func TestValidateAgainstEveryTargetBeforeAdmission(t *testing.T) {
	fixture := newRoutingFixture(t, successfulFixture(), successfulFixture(), func(c *Config, s *memoryStore) {
		p := c.Providers["secondary"]
		p.Kind = "anthropic"
		c.Providers["secondary"] = p
	})
	request := strings.TrimSuffix(input, "}") + `,"temperature":0.5,"top_p":0.5}`
	response := call(t, fixture.url, request)
	response.Body.Close()
	if response.StatusCode != 400 || fixture.primary.Load() != 0 || fixture.secondary.Load() != 0 || fixture.store.reservations != 0 {
		t.Fatal("unsupported secondary contract reached upstream/admission")
	}
}
func TestFallbackStopsWhenNewReservationIsDenied(t *testing.T) {
	fixture := newRoutingFixture(t, failStatus(503), successfulFixture(), func(c *Config, s *memoryStore) { s.balance = 108 })
	response := call(t, fixture.url, input)
	b := body(t, response)
	if response.StatusCode != 429 || fixture.secondary.Load() != 0 || fixture.store.reservations != 1 || !strings.Contains(b, "rate_limit_exceeded") {
		t.Fatalf("%d: %s", response.StatusCode, b)
	}
}
func TestNoFallbackAfterStreamCommitAndBreakerSkipsFailedPrimary(t *testing.T) {
	fixture := newRoutingFixture(t, mockprovider.New(mockprovider.Options{Chunks: 5, DropAfter: 1}), successfulFixture(), nil)
	response := call(t, fixture.url, streamInput)
	b := body(t, response)
	if response.StatusCode != 200 || !strings.Contains(b, "upstream_failure") || strings.Contains(b, "[DONE]") || fixture.secondary.Load() != 0 {
		t.Fatalf("midstream fallback: %s", b)
	}
	response = call(t, fixture.url, streamInput)
	b = body(t, response)
	if response.Header.Get("X-Gateway-Provider") != "secondary" || !strings.Contains(b, "[DONE]") || fixture.primary.Load() != 1 {
		t.Fatalf("open primary was not skipped: %s", b)
	}
}
func TestAllOpenBreakersRejectWithoutReservation(t *testing.T) {
	fixture := newRoutingFixture(t, failStatus(503), failStatus(503), nil)
	response := call(t, fixture.url, input)
	body(t, response)
	response = call(t, fixture.url, input)
	b := body(t, response)
	if response.StatusCode != 503 || !strings.Contains(b, "all_providers_unavailable") || fixture.store.reservations != 2 {
		t.Fatalf("%d: %s", response.StatusCode, b)
	}
}
func TestReconciliationFailurePreventsAnotherAttempt(t *testing.T) {
	fixture := newRoutingFixture(t, failStatus(503), successfulFixture(), func(c *Config, s *memoryStore) { s.failReconcile = true })
	response := call(t, fixture.url, input)
	b := body(t, response)
	if response.StatusCode != 503 || fixture.secondary.Load() != 0 || fixture.server.accountingErrors.Load() != 1 || !strings.Contains(b, "dependency_unavailable") {
		t.Fatalf("%d: %s", response.StatusCode, b)
	}
}
func TestStoreOutageRejectsNewCallsButNotExistingStream(t *testing.T) {
	fixture := newRoutingFixture(t, successfulFixture(), successfulFixture(), nil)
	fixture.store.mu.Lock()
	fixture.store.down = true
	fixture.store.mu.Unlock()
	response := call(t, fixture.url, input)
	body(t, response)
	if response.StatusCode != 503 || fixture.primary.Load() != 0 {
		t.Fatal("store outage admitted new work")
	}
	for path, status := range map[string]int{"/healthz": 200, "/readyz": 503} {
		response, err := http.Get(fixture.url + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != status {
			t.Fatalf("%s: %d", path, response.StatusCode)
		}
	}
	fixture.store.mu.Lock()
	fixture.store.down = false
	fixture.store.mu.Unlock()
	// Fail the store after reservation but before streaming. Neither generation
	// nor successful terminal delivery depends on further Redis availability.
	base := successfulFixture()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.store.mu.Lock()
		fixture.store.down = true
		fixture.store.mu.Unlock()
		base.ServeHTTP(w, r)
	}))
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	server, err := newServer(cfg, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	gateway := httptest.NewServer(server)
	defer gateway.Close()
	response = call(t, gateway.URL, streamInput)
	b := body(t, response)
	if response.StatusCode != 200 || !strings.Contains(b, "[DONE]") || server.accountingErrors.Load() != 1 {
		t.Fatalf("existing stream interrupted: %s", b)
	}
}
func TestTwoGatewayInstancesShareAdmission(t *testing.T) {
	address := os.Getenv("REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("set REDIS_TEST_ADDR for two-gateway acceptance")
	}
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		successfulFixture().ServeHTTP(w, r)
	}))
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		upstream.Close()
	}()
	cfg := testConfig(t, upstream.URL, "openai")
	cfg.Redis.Address = address
	cfg.Redis.Prefix = fmt.Sprintf("two-gateway:%d", time.Now().UnixNano())
	cfg.Limit = LimitConfig{Capacity: 200, RefillPerSecond: 1}
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	first := httptest.NewServer(a)
	defer first.Close()
	second := httptest.NewServer(b)
	defer second.Close()
	results := make(chan int, 20)
	for i := 0; i < 20; i++ {
		url := first.URL
		if i%2 == 0 {
			url = second.URL
		}
		go func() {
			req, _ := http.NewRequestWithContext(context.Background(), "POST", url+"/v1/chat/completions", strings.NewReader(input))
			req.Header.Set("Authorization", "Bearer gateway-test-secret")
			req.Header.Set("Content-Type", "application/json")
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				results <- 0
				return
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			results <- response.StatusCode
		}()
	}
	for i := 0; i < 19; i++ {
		select {
		case status := <-results:
			if status != 429 {
				t.Fatalf("unexpected concurrent admission status %d", status)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent admission did not finish")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("%d concurrent upstream calls from 200-token bucket", calls.Load())
	}
	close(release)
	if status := <-results; status != 200 {
		t.Fatalf("admitted call status %d", status)
	}
}

func TestFailureBeforeFirstStreamEventCanFallBack(t *testing.T) {
	first := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		http.NewResponseController(w).Flush()
	})
	fixture := newRoutingFixture(t, first, successfulFixture(), nil)
	response := call(t, fixture.url, streamInput)
	b := body(t, response)
	if response.StatusCode != 200 || response.Header.Get("X-Gateway-Provider") != "secondary" || !strings.Contains(b, "[DONE]") || fixture.store.reservations != 2 {
		t.Fatalf("pre-commit stream fallback failed: %d: %s", response.StatusCode, b)
	}
}
func TestClientCancellationDoesNotOpenBreaker(t *testing.T) {
	first := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n")
		http.NewResponseController(w).Flush()
		<-r.Context().Done()
	})
	fixture := newRoutingFixture(t, first, successfulFixture(), nil)
	response := call(t, fixture.url, streamInput)
	buf := make([]byte, 1)
	if _, err := response.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	deadline := time.Now().Add(time.Second)
	for {
		fixture.store.mu.Lock()
		settled := fixture.store.reconciliations == 1
		fixture.store.mu.Unlock()
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancellation did not settle unknown usage")
		}
		time.Sleep(time.Millisecond)
	}
	for _, breaker := range fixture.server.routing.Load().breakers {
		breaker.mu.Lock()
		state := breaker.state
		breaker.mu.Unlock()
		if state != "closed" {
			t.Fatal("client cancellation opened breaker")
		}
	}
	if fixture.secondary.Load() != 0 {
		t.Fatal("client cancellation invoked secondary")
	}
}
