package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mockprovider "github.com/rajat-blr/llm-gateway/internal/mock"
	"github.com/rajat-blr/llm-gateway/internal/storage"
)

func authorized(t *testing.T, base, method, path, key, payload string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, base+path, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+key)
	if payload != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

const newKey = `{"name":"limited","aliases":["chat"],"capacity":1000,"refill_per_second":1,"max_output_tokens":10,"allow_reuse":false}`

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestAdminKeysAuthorizationPolicyAndRevocation(t *testing.T) {
	upstream := httptest.NewServer(successfulFixture())
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	cfg.Routes["private-chat"] = cfg.Routes["chat"]
	gateway := testGateway(t, cfg)
	for _, token := range []string{"gateway-test-secret", "wrong"} {
		response := authorized(t, gateway.URL, "GET", "/admin/keys", token, "")
		if response.StatusCode != 401 {
			t.Fatal("gateway key authorized as admin")
		}
		response.Body.Close()
	}
	response := authorized(t, gateway.URL, "GET", "/v1/models", "admin-test-secret", "")
	if response.StatusCode != 401 {
		t.Fatal("admin token authorized for inference")
	}
	response.Body.Close()
	response = authorized(t, gateway.URL, "POST", "/admin/keys", "admin-test-secret", newKey)
	b := body(t, response)
	if response.StatusCode != 201 {
		t.Fatalf("%d: %s", response.StatusCode, b)
	}
	var created struct {
		storage.Key
		Secret string `json:"key"`
	}
	if json.Unmarshal([]byte(b), &created) != nil || created.Secret == "" {
		t.Fatal("missing one-time secret")
	}
	response = authorized(t, gateway.URL, "GET", "/admin/keys", "admin-test-secret", "")
	b = body(t, response)
	if strings.Contains(b, created.Secret) || strings.Contains(b, "secret_hash") || strings.Contains(b, "gateway-test-secret") {
		t.Fatal("key list leaked credentials")
	}
	response = authorized(t, gateway.URL, "GET", "/v1/models", created.Secret, "")
	b = body(t, response)
	if response.StatusCode != 200 || !strings.Contains(b, `"id":"chat"`) || strings.Contains(b, "private-chat") {
		t.Fatalf("model authorization failed: %s", b)
	}
	response = authorized(t, gateway.URL, "POST", "/v1/chat/completions", created.Secret, strings.Replace(input, "chat", "private-chat", 1))
	if response.StatusCode != 403 {
		t.Fatal("key used forbidden alias")
	}
	response.Body.Close()
	response = authorized(t, gateway.URL, "POST", "/v1/chat/completions", created.Secret, strings.TrimSuffix(input, "}")+`,"max_tokens":11}`)
	if response.StatusCode != 400 {
		t.Fatal("key exceeded output bound")
	}
	response.Body.Close()
	response = authorized(t, gateway.URL, "POST", "/v1/chat/completions", created.Secret, input)
	if response.StatusCode != 200 {
		t.Fatalf("created key failed: %s", body(t, response))
	}
	response.Body.Close()
	for i := 0; i < 2; i++ {
		response = authorized(t, gateway.URL, "DELETE", "/admin/keys/"+created.ID, "admin-test-secret", "")
		if response.StatusCode != 204 {
			t.Fatal("revocation not idempotent")
		}
		response.Body.Close()
	}
	response = authorized(t, gateway.URL, "GET", "/v1/models", created.Secret, "")
	if response.StatusCode != 401 {
		t.Fatal("revocation did not affect next request")
	}
	response.Body.Close()
}
func TestAdminValidationAndSecretSeparation(t *testing.T) {
	upstream := httptest.NewServer(successfulFixture())
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	gateway := testGateway(t, cfg)
	for _, payload := range []string{`{}`, strings.Replace(newKey, `["chat"]`, `["missing"]`, 1), strings.TrimSuffix(newKey, "}") + `,"extra":true}`, strings.Replace(newKey, `"max_output_tokens":10`, `"max_output_tokens":null`, 1), strings.Replace(newKey, `"capacity":1000`, `"capacity":0`, 1)} {
		response := authorized(t, gateway.URL, "POST", "/admin/keys", "admin-test-secret", payload)
		if response.StatusCode != 400 {
			t.Fatalf("accepted %s", payload)
		}
		response.Body.Close()
	}
	for _, path := range []string{"/admin/keys?limit=101", "/admin/keys?offset=-1", "/admin/keys?limit=1&limit=2", "/admin/usage?extra=true", "/admin/usage?from=invalid", "/admin/usage?from=2020-01-01T00:00:00Z", "/admin/usage?limit=1001"} {
		response := authorized(t, gateway.URL, "GET", path, "admin-test-secret", "")
		if response.StatusCode != 400 {
			t.Fatalf("accepted %s: %d", path, response.StatusCode)
		}
		response.Body.Close()
	}
	t.Setenv("TEST_ADMIN_TOKEN", "gateway-test-secret")
	if _, err := unitServer(t, cfg); err == nil {
		t.Fatal("shared admin and inference secret accepted")
	}
}
func TestRevokedBootstrapStaysRevokedAfterRestart(t *testing.T) {
	upstream := httptest.NewServer(successfulFixture())
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	server, err := unitServer(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	key, err := server.identities.Authenticate(context.Background(), "gateway-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	if server.identities.Revoke(context.Background(), key.ID) != nil {
		t.Fatal("revoke failed")
	}
	server.Close()
	restarted := testGateway(t, cfg)
	response := authorized(t, restarted.URL, "GET", "/v1/models", "gateway-test-secret", "")
	if response.StatusCode != 401 {
		t.Fatal("restart resurrected bootstrap")
	}
	response.Body.Close()
}
func TestUsageAPIIncludesAllAttemptsWithoutDoubleCounting(t *testing.T) {
	fixture := newRoutingFixture(t, failStatus(503), successfulFixture(), nil)
	response := call(t, fixture.url, input)
	body(t, response)
	eventually(t, func() bool { return fixture.server.usage.Stats().Written == 1 })
	response = authorized(t, fixture.url, "GET", "/admin/usage", "admin-test-secret", "")
	b := body(t, response)
	var report storage.UsageReport
	if json.Unmarshal([]byte(b), &report) != nil || response.StatusCode != 200 {
		t.Fatal(b)
	}
	if len(report.Requests) != 1 || len(report.Attempts) != 2 || report.Requests[0].ClientOutput != 3 {
		t.Fatal("incorrect client/attempt separation")
	}
	known, unknown := int64(0), int64(0)
	for _, attempt := range report.Attempts {
		known += attempt.KnownUsage
		unknown += attempt.UnknownUsage
	}
	if known != 1 || unknown != 1 {
		t.Fatal("lost failed attempt provenance")
	}
}
func TestSQLiteLossRejectsNewAuthButPreservesActiveStream(t *testing.T) {
	upstream := httptest.NewServer(mockprovider.New(mockprovider.Options{Chunks: 50, ChunkDelay: 20 * time.Millisecond}))
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	server, err := unitServer(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	gateway := httptest.NewServer(server)
	defer gateway.Close()
	active := call(t, gateway.URL, streamInput)
	server.identities.Close()
	denied := call(t, gateway.URL, input)
	if denied.StatusCode != 503 {
		t.Fatalf("SQLite loss treated as auth failure: %d", denied.StatusCode)
	}
	denied.Body.Close()
	b := body(t, active)
	if !strings.Contains(b, "[DONE]") {
		t.Fatal("SQLite loss interrupted admitted stream")
	}
	eventually(t, func() bool { return server.usage.Stats().WriteFailures == 1 })
	response := authorized(t, gateway.URL, "GET", "/admin/status", "admin-test-secret", "")
	b = body(t, response)
	var status struct {
		Available   bool          `json:"sqlite_available"`
		Persistence storage.Stats `json:"persistence"`
	}
	if json.Unmarshal([]byte(b), &status) != nil || status.Available || status.Persistence.Dropped != 1 {
		t.Fatalf("loss signals unavailable: %s", b)
	}
	response, err = http.Get(gateway.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("SQLite loss did not change readiness")
	}
}

type heldUsageSink struct {
	started chan struct{}
	release chan struct{}
}

func (s *heldUsageSink) WriteUsage(ctx context.Context, r storage.Record) error {
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return storage.ErrUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestQueueLossIsVisibleAndInferenceKeepsServing(t *testing.T) {
	upstream := httptest.NewServer(successfulFixture())
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	server, err := unitServer(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	server.usage.Drain(context.Background())
	sink := &heldUsageSink{started: make(chan struct{}, 1), release: make(chan struct{})}
	server.usage = storage.NewWorker(sink, 1)
	defer server.Close()
	defer close(sink.release)
	gateway := httptest.NewServer(server)
	defer gateway.Close()
	response := call(t, gateway.URL, input)
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	<-sink.started
	for i := 0; i < 2; i++ {
		response = call(t, gateway.URL, input)
		body(t, response)
		if response.StatusCode != 200 {
			t.Fatal("usage backpressure blocked inference")
		}
	}
	response = authorized(t, gateway.URL, "GET", "/admin/status", "admin-test-secret", "")
	b := body(t, response)
	var status struct {
		Persistence storage.Stats `json:"persistence"`
	}
	json.Unmarshal([]byte(b), &status)
	if status.Persistence.Dropped != 1 || status.Persistence.QueueDepth != 1 {
		t.Fatalf("overflow signal missing: %s", b)
	}
}

func TestAdminCanProvisionWithoutBootstrapKey(t *testing.T) {
	upstream := httptest.NewServer(successfulFixture())
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	t.Setenv("TEST_GATEWAY_KEY", "")
	gateway := testGateway(t, cfg)
	response := authorized(t, gateway.URL, "POST", "/admin/keys", "admin-test-secret", newKey)
	b := body(t, response)
	var key struct {
		Secret string `json:"key"`
	}
	if response.StatusCode != 201 || json.Unmarshal([]byte(b), &key) != nil {
		t.Fatalf("admin-only provisioning failed: %s", b)
	}
	response = authorized(t, gateway.URL, "GET", "/v1/models", key.Secret, "")
	if response.StatusCode != 200 {
		t.Fatal("provisioned key rejected")
	}
	response.Body.Close()
}
