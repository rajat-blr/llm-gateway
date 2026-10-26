package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeCandidate(t *testing.T, c Config) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing.json")
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func modelResponse(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Model string `json:"model"`
	}
	if json.NewDecoder(r.Body).Decode(&payload) != nil {
		http.Error(w, "invalid", 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": payload.Model}, "finish_reason": "stop"}}, "usage": map[string]int{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}})
}
func TestReloadPinsSnapshotThroughFallback(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	primary := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-release
		w.WriteHeader(503)
	})
	f := newRoutingFixture(t, primary, http.HandlerFunc(modelResponse), nil)
	replies := make(chan *http.Response, 1)
	go func() { replies <- call(t, f.url, input) }()
	<-started
	candidate := copyConfig(t, f.server.routing.Load().config)
	route := candidate.Routes["chat"]
	route.Revision++
	route.Targets = append([]Target(nil), route.Targets[1:]...)
	route.Targets[0].Model = "new-model"
	candidate.Routes["chat"] = route
	if err := f.server.Reload(writeCandidate(t, candidate)); err != nil {
		t.Fatal(err)
	}
	close(release)
	old := <-replies
	oldBody := body(t, old)
	if old.Header.Get("X-Config-Generation") != "1" || !strings.Contains(oldBody, "secondary-model") {
		t.Fatalf("in-flight request mixed snapshots: %s", oldBody)
	}
	current := call(t, f.url, input)
	currentBody := body(t, current)
	if current.Header.Get("X-Config-Generation") != "2" || !strings.Contains(currentBody, "new-model") || f.primary.Load() != 1 {
		t.Fatalf("new request missed published snapshot: %s", currentBody)
	}
}
func TestInvalidReloadPreservesLastGoodConfiguration(t *testing.T) {
	f := newRoutingFixture(t, successfulFixture(), successfulFixture(), nil)
	for _, change := range []func(*Config){
		func(c *Config) { r := c.Routes["chat"]; r.MaxOutputTokens--; c.Routes["chat"] = r },
		func(c *Config) {
			r := c.Routes["chat"]
			r.Revision++
			r.Targets[0].Provider = "missing"
			c.Routes["chat"] = r
		},
		func(c *Config) { c.MaxConcurrent++ },
		func(c *Config) {
			p := c.Providers["fixture"]
			p.KeyEnv = "DIFFERENT_CREDENTIAL"
			c.Providers["fixture"] = p
		},
		func(c *Config) {
			r := c.Routes["chat"]
			r.Revision++
			r.Cache = CachePolicy{Enabled: true, TTL: Duration(time.Minute), MaxEntryBytes: 1}
			c.Routes["chat"] = r
		},
	} {
		candidate := copyConfig(t, f.server.routing.Load().config)
		change(&candidate)
		if err := f.server.Reload(writeCandidate(t, candidate)); err == nil {
			t.Fatal("invalid reload accepted")
		}
		if f.server.routing.Load().generation != 1 {
			t.Fatal("invalid reload replaced live snapshot")
		}
	}
	path := filepath.Join(t.TempDir(), "invalid.json")
	os.WriteFile(path, []byte(`{"routes":`), 0600)
	if f.server.Reload(path) == nil {
		t.Fatal("malformed JSON accepted")
	}
	response := call(t, f.url, input)
	body(t, response)
	if response.StatusCode != 200 || response.Header.Get("X-Config-Generation") != "1" {
		t.Fatal("invalid reload broke last good config")
	}
	if f.server.Reload("../../configs/invalid-reload.json") == nil {
		t.Fatal("checked-in invalid example accepted")
	}
	if f.server.reloadRejected.Load() != 7 {
		t.Fatal("reload rejection not visible")
	}
}
func TestSnapshotDetachedAndUnchangedBreakersRetained(t *testing.T) {
	f := newRoutingFixture(t, successfulFixture(), successfulFixture(), nil)
	old := f.server.routing.Load()
	candidate := copyConfig(t, old.config)
	r := candidate.Routes["chat"]
	r.Revision++
	r.MaxOutputTokens--
	candidate.Routes["chat"] = r
	if err := f.server.publish(candidate); err != nil {
		t.Fatal(err)
	}
	candidate.Routes["chat"].Targets[0].Model = "mutated-after-publish"
	current := f.server.routing.Load()
	if current.config.Routes["chat"].Targets[0].Model == "mutated-after-publish" {
		t.Fatal("published snapshot shares mutable caller data")
	}
	for identity, breaker := range old.breakers {
		if current.breakers[identity] != breaker {
			t.Fatal("unchanged upstream lost breaker state")
		}
	}
}
