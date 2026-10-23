// Package mock serves deterministic OpenAI and Anthropic protocol fixtures.
package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Options struct {
	FirstDelay time.Duration
	ChunkDelay time.Duration
	Chunks     int
	Payload    string
	FailEvery  uint64
	DropAfter  int
}
type Server struct {
	options  Options
	calls    atomic.Uint64
	canceled atomic.Uint64
}

func New(o Options) *Server {
	if o.Chunks < 1 {
		o.Chunks = 3
	}
	if o.Payload == "" {
		o.Payload = "hello "
	}
	return &Server{options: o}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && r.URL.Path == "/healthz" {
		w.WriteHeader(200)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/stats" {
		_ = json.NewEncoder(w).Encode(map[string]uint64{"calls": s.calls.Load(), "canceled": s.canceled.Load()})
		return
	}
	if r.Method != "POST" || (r.URL.Path != "/v1/messages" && r.URL.Path != "/v1/chat/completions") {
		http.NotFound(w, r)
		return
	}
	call := s.calls.Add(1)
	if s.options.FailEvery > 0 && call%s.options.FailEvery == 0 {
		http.Error(w, "seeded failure", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request struct {
		Stream bool `json:"stream"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if !wait(r.Context(), s.options.FirstDelay) {
		s.canceled.Add(1)
		return
	}
	anthropic := r.URL.Path == "/v1/messages"
	if !request.Stream {
		w.Header().Set("Content-Type", "application/json")
		text := ""
		for i := 0; i < s.options.Chunks; i++ {
			text += s.options.Payload
		}
		var response any
		if anthropic {
			response = map[string]any{"id": "mock-upstream-id", "type": "message", "role": "assistant", "content": []any{map[string]any{"type": "text", "text": text}}, "model": "mock-text", "stop_reason": "end_turn", "usage": map[string]int{"input_tokens": 5, "output_tokens": s.options.Chunks}}
		} else {
			response = map[string]any{"id": "mock-upstream-id", "object": "chat.completion", "model": "mock-text", "choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": text}, "finish_reason": "stop"}}, "usage": map[string]int{"prompt_tokens": 5, "completion_tokens": s.options.Chunks, "total_tokens": 5 + s.options.Chunks}}
		}
		_ = json.NewEncoder(w).Encode(response)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	send := func(name string, v any) bool {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
		b, _ := json.Marshal(v)
		var err error
		if name != "" {
			_, err = fmt.Fprintf(w, "event: %s\n", name)
			if err != nil {
				return false
			}
		}
		_, err = fmt.Fprintf(w, "data: %s\n\n", b)
		if err != nil {
			return false
		}
		return http.NewResponseController(w).Flush() == nil
	}
	if anthropic {
		if !send("message_start", map[string]any{"type": "message_start", "message": map[string]any{"role": "assistant", "usage": map[string]int{"input_tokens": 5, "output_tokens": 0}}}) {
			return
		}
		if !send("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]string{"type": "text", "text": ""}}) {
			return
		}
	} else {
		if !send("", openAIChunk(map[string]string{"role": "assistant"}, nil)) {
			return
		}
	}
	for i := 0; i < s.options.Chunks; i++ {
		if s.options.DropAfter > 0 && i == s.options.DropAfter {
			return
		}
		if !wait(r.Context(), s.options.ChunkDelay) {
			s.canceled.Add(1)
			return
		}
		if anthropic {
			if !send("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "text_delta", "text": s.options.Payload}}) {
				return
			}
		} else {
			if !send("", openAIChunk(map[string]string{"content": s.options.Payload}, nil)) {
				return
			}
		}
	}
	if anthropic {
		if !send("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}) {
			return
		}
		if !send("message_delta", map[string]any{"type": "message_delta", "delta": map[string]string{"stop_reason": "end_turn"}, "usage": map[string]int{"output_tokens": s.options.Chunks}}) {
			return
		}
		send("message_stop", map[string]string{"type": "message_stop"})
	} else {
		if !send("", openAIChunk(map[string]string{}, "stop")) {
			return
		}
		if !send("", map[string]any{"choices": []any{}, "usage": map[string]int{"prompt_tokens": 5, "completion_tokens": s.options.Chunks, "total_tokens": 5 + s.options.Chunks}}) {
			return
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		_ = http.NewResponseController(w).Flush()
	}
}
func openAIChunk(delta any, finish any) any {
	return map[string]any{"id": "mock-upstream-id", "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
