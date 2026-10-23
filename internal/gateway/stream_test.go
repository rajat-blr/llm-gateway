package gateway

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestStreamTerminalValidation(t *testing.T) {
	fixtures := []struct {
		name, kind, body string
		success          bool
	}{
		{"openai normal", "openai", "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", true},
		{"missing terminal", "openai", "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", false},
		{"missing finish", "openai", "data: [DONE]\n\n", false},
		{"upstream error", "openai", "data: {\"error\":{\"message\":\"secret\"}}\n\n", false},
		{"tool call", "openai", "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{}]},\"finish_reason\":null}]}\n\n", false},
		{"invalid finish", "openai", "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n", false},
		{"truncated event", "openai", "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}", false},
		{"anthropic missing finish", "anthropic", "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", false},
		{"anthropic mismatched event", "anthropic", "event: ping\ndata: {\"type\":\"message_stop\"}\n\n", false},
		{"anthropic premature delta", "anthropic", "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n", false},
	}
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			stream := newStream(io.NopCloser(strings.NewReader(f.body)), f.kind, 1024, 8192)
			defer stream.Close()
			done := false
			for i := 0; i < 10; i++ {
				event, err := stream.Next(context.Background())
				if err != nil {
					break
				}
				if event.Done {
					done = true
					if _, err := stream.Next(context.Background()); err != io.EOF {
						t.Fatal("stream did not stop after terminal")
					}
					break
				}
			}
			if done != f.success {
				t.Fatalf("success=%v, expected %v", done, f.success)
			}
		})
	}
}
func TestSSEFramingAndByteBudget(t *testing.T) {
	fixture := ": heartbeat\r\nevent: ignored\r\ndata: {\"choices\":\r\ndata: [{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	stream := newStream(io.NopCloser(strings.NewReader(fixture)), "openai", 1024, 4096)
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err := stream.Next(context.Background())
	if err != nil || !event.Done {
		t.Fatalf("%+v: %v", event, err)
	}
	stream = newStream(io.NopCloser(strings.NewReader(fixture)), "openai", 1024, 20)
	if _, err := stream.Next(context.Background()); err == nil {
		t.Fatal("accepted stream exceeding byte budget")
	}
	stream = newStream(io.NopCloser(strings.NewReader(strings.Repeat(": heartbeat\n\n", 1000))), "openai", 128, 100)
	if _, err := stream.Next(context.Background()); err == nil {
		t.Fatal("comments bypassed byte budget")
	}
}
func TestCompletionRejectsUnsupportedUpstreamPayload(t *testing.T) {
	for _, b := range []string{
		`{"choices":[{"index":1,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"role":"assistant","content":null},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"role":"assistant","content":"x","tool_calls":[{}]},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":1}}`,
	} {
		if _, _, _, err := decodeCompletion([]byte(b), "openai"); err == nil {
			t.Fatalf("accepted %s", b)
		}
	}
	if _, _, _, err := decodeCompletion([]byte(`{"type":"message","role":"assistant","content":[{"type":"thinking","text":"x"}],"stop_reason":"end_turn"}`), "anthropic"); err == nil {
		t.Fatal("unsupported Anthropic block accepted")
	}
}
func FuzzStrictRequest(f *testing.F) {
	f.Add([]byte(input))
	f.Add([]byte(streamInput))
	f.Add([]byte(`{"messages":null}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<16 {
			t.Skip()
		}
		var r Request
		if StrictJSON(b, &r) == nil {
			r.Validate(Route{MaxOutputTokens: 100}, ProviderConfig{Kind: "openai"})
			if _, err := json.Marshal(r); err != nil {
				t.Fatal(err)
			}
		}
	})
}
