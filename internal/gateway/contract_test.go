package gateway

import (
	"strings"
	"testing"
)

func TestRequestContract(t *testing.T) {
	route := Route{MaxOutputTokens: 100}
	valid := []string{
		`{"model":"a","messages":[{"role":"user","content":" hi "}]}`,
		`{"model":"a","messages":[{"role":"user","content":"hi"}],"stop":"end","n":1,"temperature":0,"top_p":1,"max_tokens":1,"stream":true,"stream_options":{"include_usage":true}}`,
	}
	invalid := []string{
		`null`, `{}`, `{"model":"a","messages":[]}`, `{"model":"a","messages":[{"role":"user","content":""}]}`,
		`{"model":"a","messages":[{"role":"tool","content":"hi"}]}`,
		`{"model":"a","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`,
	}
	base := `{"model":"a","messages":[{"role":"user","content":"hi"}]`
	for _, suffix := range []string{`,"temperature":null}`, `,"temperature":1.01}`, `,"top_p":0}`, `,"max_tokens":101}`, `,"max_tokens":0}`, `,"max_tokens":1.5}`, `,"n":2}`, `,"stop":[]}`, `,"stop":""}`, `,"stop":["ok",""]}`, `,"stream_options":{}}`, `,"tools":[]}`, `,"model":"b"}`, `,"stream":"true"}`, `,"stop":["a","b","c","d","e"]}`, `,"stream":true,"stream_options":{"foo":true}}`, `,"stream":true,"stream_options":{"include_usage":null}}`} {
		invalid = append(invalid, base+suffix)
	}
	invalid = append(invalid, valid[0]+` {}`, strings.Replace(valid[0], `"content":" hi "`, `"content":"a","content":"b"`, 1))
	for _, body := range valid {
		t.Run(body, func(t *testing.T) {
			var r Request
			if err := StrictJSON([]byte(body), &r); err != nil {
				t.Fatal(err)
			}
			if err := r.Validate(route, ProviderConfig{Kind: "openai"}); err != nil {
				t.Fatal(err)
			}
			if r.MaxTokens == nil {
				t.Fatal("missing default output bound")
			}
		})
	}
	for _, body := range invalid {
		t.Run(body, func(t *testing.T) {
			var r Request
			err := StrictJSON([]byte(body), &r)
			if err == nil {
				err = r.Validate(route, ProviderConfig{Kind: "openai"})
			}
			if err == nil {
				t.Fatal("accepted unsupported input")
			}
		})
	}
}
func TestAnthropicContract(t *testing.T) {
	for _, roles := range [][]string{{"system"}, {"assistant", "user"}, {"user", "system", "user"}, {"user", "assistant"}} {
		r := Request{Model: "a"}
		for _, role := range roles {
			r.Messages = append(r.Messages, Message{role, "text"})
		}
		if r.Validate(Route{MaxOutputTokens: 100}, ProviderConfig{Kind: "anthropic"}) == nil {
			t.Fatalf("accepted %v", roles)
		}
	}
	x := 0.5
	r := Request{Model: "a", Messages: []Message{{"user", "hi"}}, Temperature: &x, TopP: &x}
	if r.Validate(Route{MaxOutputTokens: 100}, ProviderConfig{Kind: "anthropic"}) == nil {
		t.Fatal("accepted conflicting sampling options")
	}
}
func TestConfigValidation(t *testing.T) {
	cfg, err := LoadConfig("../../configs/demo.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.MaxStreams = c.MaxConcurrent + 1 }, func(c *Config) { c.IdleTimeout = c.RequestTimeout + 1 }, func(c *Config) { c.MaxOutputBytes = 0 }, func(c *Config) {
			c.Routes["bad"] = Route{Targets: []Target{{Provider: "missing", Model: "x", AttemptTimeout: c.RequestTimeout}}, MaxOutputTokens: 1}
		},
	} {
		copy := cfg
		copy.Routes = map[string]Route{}
		for k, v := range cfg.Routes {
			copy.Routes[k] = v
		}
		mutate(&copy)
		if copy.Validate() == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
}
