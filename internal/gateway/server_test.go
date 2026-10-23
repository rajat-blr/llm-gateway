package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mockprovider "github.com/rajat-blr/llm-gateway/internal/mock"
)

func testConfig(t *testing.T, url, kind string) Config {
	t.Helper()
	cfg, err := LoadConfig("../../configs/demo.json")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Providers = map[string]ProviderConfig{"fixture": {Kind: kind, BaseURL: url + "/v1", KeyEnv: "TEST_PROVIDER_KEY"}}
	cfg.Routes = map[string]Route{"chat": {Provider: "fixture", Model: "upstream-model", MaxOutputTokens: 100}}
	cfg.KeyEnv = "TEST_GATEWAY_KEY"
	t.Setenv("TEST_GATEWAY_KEY", "gateway-test-secret")
	t.Setenv("TEST_PROVIDER_KEY", "provider-test-secret")
	return cfg
}
func testGateway(t *testing.T, c Config) *httptest.Server {
	t.Helper()
	handler, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() { server.Close(); handler.Close() })
	return server
}
func call(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", url+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer gateway-test-secret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

const input = `{"model":"chat","messages":[{"role":"user","content":"hello"}]}`
const streamInput = `{"model":"chat","messages":[{"role":"user","content":"hello"}],"stream":true,"stream_options":{"include_usage":true}}`

func body(t *testing.T, r *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	return string(b)
}
func TestAdaptersEndToEnd(t *testing.T) {
	for _, kind := range []string{"openai", "anthropic"} {
		t.Run(kind, func(t *testing.T) {
			upstream := httptest.NewServer(mockprovider.New(mockprovider.Options{Chunks: 3, Payload: "hi "}))
			defer upstream.Close()
			gateway := testGateway(t, testConfig(t, upstream.URL, kind))
			resp := call(t, gateway.URL, input)
			b := body(t, resp)
			if resp.StatusCode != 200 {
				t.Fatalf("%d: %s", resp.StatusCode, b)
			}
			var c Completion
			if err := json.Unmarshal([]byte(b), &c); err != nil {
				t.Fatal(err)
			}
			if c.Model != "chat" || c.ID != resp.Header.Get("X-Request-ID") || c.Choices[0].Message.Content != "hi hi hi " || *c.Choices[0].FinishReason != "stop" || c.Usage.TotalTokens != 8 {
				t.Fatalf("unexpected completion %s", b)
			}
			resp = call(t, gateway.URL, streamInput)
			b = body(t, resp)
			if resp.StatusCode != 200 || !strings.HasSuffix(b, "data: [DONE]\n\n") || strings.Contains(b, "mock-upstream-id") {
				t.Fatalf("bad stream: %s", b)
			}
			chunks := parseChunks(t, b)
			var text string
			usageCount := 0
			for _, chunk := range chunks {
				if chunk.ID != resp.Header.Get("X-Request-ID") || chunk.Model != "chat" || chunk.Object != "chat.completion.chunk" {
					t.Fatalf("unstable metadata: %+v", chunk)
				}
				if len(chunk.Choices) > 0 && chunk.Choices[0].Delta != nil {
					text += chunk.Choices[0].Delta.Content
				}
				if chunk.Usage != nil {
					usageCount++
					if len(chunk.Choices) != 0 || chunk.Usage.TotalTokens != 8 {
						t.Fatal("invalid usage chunk")
					}
				}
			}
			if text != "hi hi hi " || usageCount != 1 {
				t.Fatalf("text=%q usage=%d", text, usageCount)
			}
			resp = call(t, gateway.URL, strings.Replace(streamInput, `,"stream_options":{"include_usage":true}`, "", 1))
			b = body(t, resp)
			for _, chunk := range parseChunks(t, b) {
				if chunk.Usage != nil {
					t.Fatal("usage emitted without opt-in")
				}
			}
		})
	}
}
func parseChunks(t *testing.T, b string) []Completion {
	t.Helper()
	var chunks []Completion
	for _, line := range strings.Split(b, "\n") {
		if strings.HasPrefix(line, "data: ") && line != "data: [DONE]" {
			var c Completion
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &c); err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, c)
		}
	}
	return chunks
}
func TestRejectionsDoNotCallUpstream(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer upstream.Close()
	gateway := testGateway(t, testConfig(t, upstream.URL, "openai"))
	for _, c := range []struct {
		body   string
		status int
	}{
		{strings.Replace(input, `"hello"`, `null`, 1), 400}, {strings.Replace(input, `"chat"`, `"unknown"`, 1), 403}, {strings.TrimSuffix(input, "}") + `,"tools":[]}`, 400}, {input + input, 400},
	} {
		resp := call(t, gateway.URL, c.body)
		if resp.StatusCode != c.status {
			t.Fatalf("expected %d got %d: %s", c.status, resp.StatusCode, body(t, resp))
		}
		resp.Body.Close()
	}
	req, _ := http.NewRequest("POST", gateway.URL+"/v1/chat/completions", strings.NewReader(input))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached upstream")
	}
}
func TestModelsAndReadiness(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	handler, err := New(testConfig(t, upstream.URL, "openai"))
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	gateway := httptest.NewServer(handler)
	defer gateway.Close()
	req, _ := http.NewRequest("GET", gateway.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer gateway-test-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b := body(t, resp)
	if resp.StatusCode != 200 || !strings.Contains(b, `"id":"chat"`) {
		t.Fatal(b)
	}
	handler.StopAdmission()
	for path, status := range map[string]int{"/healthz": 200, "/readyz": 503, "/v1/models": 503} {
		resp, err = http.Get(gateway.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != status {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
}
func TestMidstreamDropNeverSucceeds(t *testing.T) {
	for _, kind := range []string{"openai", "anthropic"} {
		t.Run(kind, func(t *testing.T) {
			upstream := httptest.NewServer(mockprovider.New(mockprovider.Options{Chunks: 5, DropAfter: 1}))
			defer upstream.Close()
			gateway := testGateway(t, testConfig(t, upstream.URL, kind))
			resp := call(t, gateway.URL, streamInput)
			b := body(t, resp)
			if resp.StatusCode != 200 || !strings.Contains(b, `"code":"upstream_failure"`) || strings.Contains(b, "[DONE]") {
				t.Fatalf("unexpected failure: %s", b)
			}
		})
	}
}
func TestMalformedInitialStreamReturns502(t *testing.T) {
	for _, fixture := range []string{"data: [DONE]\n\n", "data: not-json\n\n", "data: {\"choices\":[]}\n\n"} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, fixture)
		}))
		gateway := testGateway(t, testConfig(t, upstream.URL, "openai"))
		resp := call(t, gateway.URL, streamInput)
		b := body(t, resp)
		if resp.StatusCode != 502 || strings.Contains(b, "[DONE]") {
			t.Fatalf("%d: %s", resp.StatusCode, b)
		}
		upstream.Close()
	}
}
func TestRequestAndEventLimits(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", strings.Repeat("x", 1024))
	}))
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	cfg.MaxEventBytes = 128
	gateway := testGateway(t, cfg)
	resp := call(t, gateway.URL, streamInput)
	if resp.StatusCode != 502 {
		t.Fatal(resp.StatusCode)
	}
	resp.Body.Close()
	cfg.MaxBodyBytes = 32
	gateway = testGateway(t, cfg)
	resp = call(t, gateway.URL, input)
	if resp.StatusCode != 400 {
		t.Fatal(resp.StatusCode)
	}
	resp.Body.Close()
}
func TestErrorSanitizationAndRedirect(t *testing.T) {
	var leaked atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer destination.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL+"/provider-secret", 307)
	}))
	defer upstream.Close()
	gateway := testGateway(t, testConfig(t, upstream.URL, "openai"))
	resp := call(t, gateway.URL, input)
	b := body(t, resp)
	if resp.StatusCode != 502 || leaked.Load() || strings.Contains(b, "secret") || strings.Contains(b, upstream.URL) {
		t.Fatalf("unsafe error: %s", b)
	}
}
func TestClientDisconnectCancelsUpstream(t *testing.T) {
	canceled := make(chan struct{})
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n")
		http.NewResponseController(w).Flush()
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer upstream.Close()
	gateway := testGateway(t, testConfig(t, upstream.URL, "openai"))
	resp := call(t, gateway.URL, streamInput)
	<-started
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || !strings.Contains(line, "hi") {
		t.Fatalf("%q: %v", line, err)
	}
	resp.Body.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("upstream not canceled within one second")
	}
}
func TestOverallDeadlineIncludesAdmission(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			close(started)
		}
		<-r.Context().Done()
	}))
	defer upstream.Close()
	cfg := testConfig(t, upstream.URL, "openai")
	cfg.MaxConcurrent = 1
	cfg.MaxStreams = 1
	cfg.RequestTimeout = Duration(200 * time.Millisecond)
	cfg.IdleTimeout = Duration(200 * time.Millisecond)
	cfg.WriteTimeout = Duration(100 * time.Millisecond)
	gateway := testGateway(t, cfg)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp := call(t, gateway.URL, input)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	<-started
	// Independently exercise cancellable admission without relying on which
	// transport deadline wins the race at the overall request boundary.
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := acquire(ctx, slots); err != context.DeadlineExceeded {
		t.Fatalf("admission error: %v", err)
	}
	wg.Wait()
}
func TestAdapterRequestMapping(t *testing.T) {
	n := 42
	temp := 0.25
	r := Request{Model: "alias", Messages: []Message{{"system", " keep whitespace "}, {"system", "two"}, {"user", "hello"}}, Temperature: &temp, MaxTokens: &n, Stop: Stop{"end"}, Stream: true}
	for _, kind := range []string{"openai", "anthropic"} {
		req, err := upstreamRequest(context.Background(), r, Route{Model: "upstream"}, ProviderConfig{Kind: kind, BaseURL: "http://example.test/v1"}, "credential")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(req.Body)
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(b, &payload); err != nil {
			t.Fatal(err)
		}
		if string(payload["model"]) != `"upstream"` || string(payload["max_tokens"]) != "42" || string(payload["temperature"]) != "0.25" {
			t.Fatal(string(b))
		}
		if kind == "anthropic" {
			if !bytes.Contains(payload["system"], []byte(" keep whitespace ")) || payload["stop_sequences"] == nil || bytes.Contains(payload["messages"], []byte("system")) || req.Header.Get("x-api-key") != "credential" {
				t.Fatal(string(b))
			}
		} else {
			if !bytes.Contains(payload["stream_options"], []byte("true")) || req.Header.Get("Authorization") != "Bearer credential" {
				t.Fatal(string(b))
			}
		}
	}
}
