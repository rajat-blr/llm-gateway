package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mockprovider "github.com/rajat-blr/llm-gateway/internal/mock"
)

func metricSum(t *testing.T, s *Server, name string) float64 {
	t.Helper()
	families, err := s.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.Metric {
			switch {
			case m.Counter != nil:
				total += m.Counter.GetValue()
			case m.Gauge != nil:
				total += m.Gauge.GetValue()
			case m.Histogram != nil:
				total += float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return total
}
func managementCall(s *Server, method, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.ManagementHandler().ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}
func TestMetricsSeparateListenerAndExecutionOwnership(t *testing.T) {
	f := cacheHarness(t, mockprovider.New(mockprovider.Options{Chunks: 3, Payload: "hi "}), nil)
	for i := 0; i < 2; i++ {
		resp := cachedCall(t, f, input)
		if resp.StatusCode != 200 {
			t.Fatal(body(t, resp))
		}
		body(t, resp)
	}
	if got := metricSum(t, f.server, "llmgw_requests_total"); got != 2 {
		t.Fatalf("requests %v", got)
	}
	if got := metricSum(t, f.server, "llmgw_attempts_total"); got != 1 {
		t.Fatalf("attempts %v", got)
	}
	if got := metricSum(t, f.server, "llmgw_provider_tokens_total"); got != 8 {
		t.Fatalf("tokens %v", got)
	}
	if got := metricSum(t, f.server, "llmgw_cache_hits_total"); got != 1 {
		t.Fatalf("cache hits %v", got)
	}
	resp, err := http.Get(f.url + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode == 200 {
		t.Fatal("metrics exposed on inference listener")
	}
	body(t, resp)
	w := managementCall(f.server, "GET", "/metrics")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "go_goroutines") {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, secret := range []string{f.secret, "provider-test-secret", "admin-test-secret", "upstream-model"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("sensitive metric label", secret)
		}
	}
	if w := managementCall(f.server, "GET", "/debug/pprof/"); w.Code != 404 {
		t.Fatal("profiling enabled by default", w.Code)
	}
	if w := managementCall(f.server, "POST", "/metrics"); w.Code != 405 {
		t.Fatal(w.Code)
	}
}
func TestMetricsStreamFirstTextAndAuthentication(t *testing.T) {
	upstream := httptest.NewServer(mockprovider.New(mockprovider.Options{Chunks: 3, Payload: "hi ", ChunkDelay: time.Millisecond}))
	defer upstream.Close()
	s, err := unitServer(t, testConfig(t, upstream.URL, "openai"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	server := httptest.NewServer(s)
	defer server.Close()
	resp := call(t, server.URL, streamInput)
	if resp.StatusCode != 200 {
		t.Fatal(body(t, resp))
	}
	body(t, resp)
	if got := metricSum(t, s, "llmgw_first_text_seconds"); got != 1 {
		t.Fatalf("first text observations %v", got)
	}
	if got := metricSum(t, s, "llmgw_active_attempts"); got != 0 {
		t.Fatal("leaked attempts", got)
	}
	if got := metricSum(t, s, "llmgw_active_streams"); got != 0 {
		t.Fatal("leaked streams", got)
	}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(input))
	s.ServeHTTP(httptest.NewRecorder(), req)
	if got := metricSum(t, s, "llmgw_requests_total"); got != 2 {
		t.Fatal("missing auth failure", got)
	}
	if got := metricSum(t, s, "llmgw_attempts_total"); got != 1 {
		t.Fatal("auth failure counted as upstream attempt", got)
	}
}
func TestMetricLabelsBoundedAcrossReloads(t *testing.T) {
	upstream := httptest.NewServer(mockprovider.New(mockprovider.Options{Chunks: 1, Payload: "hi"}))
	defer upstream.Close()
	s, err := unitServer(t, testConfig(t, upstream.URL, "openai"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 40; i++ {
		c := s.cfg
		name := fmt.Sprintf("new-alias-%d", i)
		c.Routes = map[string]Route{name: s.cfg.Routes["chat"]}
		if err := s.publish(c); err != nil {
			t.Fatal(err)
		}
		s.metrics.requests.WithLabelValues(s.metrics.alias(name), "json", "success", "BYPASS").Inc()
	}
	families, err := s.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "llmgw_requests_total" {
			if len(f.Metric) != 1 || f.Metric[0].Label[0].GetValue() != "other" {
				t.Fatalf("unbounded labels %v", f)
			}
		}
	}
	if got := metricSum(t, s, "llmgw_reload_accepted_total"); got != 40 {
		t.Fatal(got)
	}
}
func TestManagementValidationAndProfiles(t *testing.T) {
	for _, c := range []ManagementConfig{{Listen: ":9095"}, {Listen: "0.0.0.0:9095", AllowRemote: true, Pprof: true}, {Listen: "127.0.0.1:9095", AllowRemote: true, Pprof: true}, {Pprof: true}, {Listen: "bad"}} {
		if c.Validate() == nil {
			t.Fatal("accepted", c)
		}
	}
	for _, c := range []ManagementConfig{{}, {Listen: "127.0.0.1:0", Pprof: true}, {Listen: "[::1]:9095"}, {Listen: ":9095", AllowRemote: true}} {
		if err := c.Validate(); err != nil {
			t.Fatal(c, err)
		}
	}
	upstream := httptest.NewServer(mockprovider.New(mockprovider.Options{}))
	defer upstream.Close()
	c := testConfig(t, upstream.URL, "openai")
	c.Management.Pprof = true
	s, err := unitServer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1"} {
		w := managementCall(s, "GET", path)
		if w.Code != 200 {
			t.Fatal(path, w.Code)
		}
	}
	for _, path := range []string{"/debug/pprof/profile?seconds=1000", "/debug/pprof/trace?seconds=-1", "/debug/pprof/heap?seconds=999", "/debug/pprof/profile?seconds=no"} {
		w := managementCall(s, "GET", path)
		if w.Code != 400 {
			t.Fatal(path, w.Code)
		}
	}
}
func TestFirstTextExcludesControlEvents(t *testing.T) {
	// A role-only stream completes without text and must not report TTFT.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	s, err := unitServer(t, testConfig(t, upstream.URL, "openai"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	server := httptest.NewServer(s)
	defer server.Close()
	response := call(t, server.URL, streamInput)
	output := body(t, response)
	if response.StatusCode != 200 || !strings.HasSuffix(output, "data: [DONE]\n\n") {
		t.Fatal(response.StatusCode, output)
	}
	if got := metricSum(t, s, "llmgw_first_text_seconds"); got != 0 {
		t.Fatal(got)
	}
}
