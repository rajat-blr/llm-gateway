package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Stop []string

func (s *Stop) UnmarshalJSON(b []byte) error {
	var single string
	if json.Unmarshal(b, &single) == nil {
		*s = []string{single}
		return nil
	}
	var multiple []string
	if err := json.Unmarshal(b, &multiple); err != nil {
		return err
	}
	*s = multiple
	return nil
}

type Request struct {
	Model         string         `json:"model"`
	Messages      []Message      `json:"messages"`
	Stream        bool           `json:"stream,omitempty"`
	Temperature   *float64       `json:"temperature,omitempty"`
	TopP          *float64       `json:"top_p,omitempty"`
	MaxTokens     *int           `json:"max_tokens,omitempty"`
	Stop          Stop           `json:"stop,omitempty"`
	N             *int           `json:"n,omitempty"`
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
}
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (u Usage) valid() bool {
	return u.TotalTokens <= 1_000_000_000 && u.PromptTokens >= 0 && u.CompletionTokens >= 0 && u.TotalTokens >= u.PromptTokens && u.TotalTokens-u.PromptTokens == u.CompletionTokens
}

type Completion struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   *Usage   `json:"usage,omitempty"`
}
type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	Delta        *Delta   `json:"delta,omitempty"`
	FinishReason *string  `json:"finish_reason"`
}
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}
type APIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}
type ErrorEnvelope struct {
	Error APIError `json:"error"`
}

func apiError(code string) ErrorEnvelope {
	messages := map[string]string{
		"rate_limit_exceeded":       "The shared token budget is exhausted.",
		"all_providers_unavailable": "All configured providers are unavailable.",
		"invalid_request":           "Invalid or unsupported request.", "invalid_api_key": "A valid bearer key is required.",
		"model_not_allowed": "The requested model alias is unavailable.", "upstream_failure": "The upstream provider failed.",
		"deadline_exceeded": "The request deadline expired.", "dependency_unavailable": "The gateway is unavailable.",
	}
	return ErrorEnvelope{APIError{Message: messages[code], Type: code, Code: code}}
}

// StrictJSON rejects duplicate members and null at every depth, in addition to
// unknown fields and trailing JSON. This prevents ambiguous adapter mappings.
func StrictJSON(b []byte, dst any) error {
	// Validate syntax first, then inspect structure without materializing string
	// values or number tokens. Only object keys need decoding for duplicate checks.
	if !json.Valid(b) {
		return errors.New("invalid JSON")
	}
	scan := strictScanner{data: b}
	if err := scan.value(0); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(dst)
}

type strictScanner struct {
	data []byte
	pos  int
}

func (s *strictScanner) space() {
	for s.pos < len(s.data) {
		switch s.data[s.pos] {
		case ' ', '\t', '\r', '\n':
			s.pos++
		default:
			return
		}
	}
}

// Syntax has already been checked by json.Valid. Escaped quotes therefore
// always have a following byte and cannot terminate a string.
func (s *strictScanner) stringEnd() {
	s.pos++
	for s.data[s.pos] != '"' {
		if s.data[s.pos] == '\\' {
			s.pos++
		}
		s.pos++
	}
	s.pos++
}
func (s *strictScanner) value(depth int) error {
	if depth > 32 {
		return errors.New("JSON nesting limit")
	}
	s.space()
	switch s.data[s.pos] {
	case 'n':
		return errors.New("null is unsupported")
	case '"':
		s.stringEnd()
		return nil
	case '{':
		s.pos++
		s.space()
		seen := map[string]bool{}
		if s.data[s.pos] == '}' {
			s.pos++
			return nil
		}
		for {
			start := s.pos
			s.stringEnd()
			var key string
			raw := s.data[start+1 : s.pos-1]
			if bytes.IndexByte(raw, '\\') < 0 && utf8.Valid(raw) {
				key = string(raw)
			} else if err := json.Unmarshal(s.data[start:s.pos], &key); err != nil {
				return err
			}
			if seen[key] {
				return errors.New("duplicate JSON member")
			}
			seen[key] = true
			s.space()
			s.pos++ // colon
			if err := s.value(depth + 1); err != nil {
				return err
			}
			s.space()
			if s.data[s.pos] == '}' {
				s.pos++
				return nil
			}
			s.pos++
			s.space()
		}
	case '[':
		s.pos++
		s.space()
		if s.data[s.pos] == ']' {
			s.pos++
			return nil
		}
		for {
			if err := s.value(depth + 1); err != nil {
				return err
			}
			s.space()
			if s.data[s.pos] == ']' {
				s.pos++
				return nil
			}
			s.pos++
		}
	default:
		for s.pos < len(s.data) {
			switch s.data[s.pos] {
			case ' ', '\t', '\r', '\n', ',', ']', '}':
				return nil
			default:
				s.pos++
			}
		}
		return nil
	}
}
func (r *Request) Validate(route Route, provider ProviderConfig) error {
	if r.Model == "" || len(r.Messages) == 0 || len(r.Messages) > 256 {
		return errors.New("model and messages required")
	}
	if r.N != nil && *r.N != 1 {
		return errors.New("only one completion supported")
	}
	if r.Temperature != nil && (math.IsNaN(*r.Temperature) || *r.Temperature < 0 || *r.Temperature > 1) {
		return errors.New("temperature outside 0..1")
	}
	if r.TopP != nil && (math.IsNaN(*r.TopP) || *r.TopP <= 0 || *r.TopP > 1) {
		return errors.New("top_p outside (0,1]")
	}
	if r.MaxTokens == nil {
		n := route.MaxOutputTokens
		r.MaxTokens = &n
	}
	if *r.MaxTokens < 1 || *r.MaxTokens > route.MaxOutputTokens {
		return errors.New("max_tokens outside route bound")
	}
	if r.StreamOptions != nil && !r.Stream {
		return errors.New("stream_options requires stream")
	}
	if r.Stop != nil && (len(r.Stop) == 0 || len(r.Stop) > 4) {
		return errors.New("stop must have 1..4 entries")
	}
	for _, s := range r.Stop {
		if s == "" {
			return errors.New("empty stop")
		}
	}
	conversation := false
	for _, m := range r.Messages {
		if m.Content == "" {
			return errors.New("empty content")
		}
		switch m.Role {
		case "system", "user", "assistant":
		default:
			return errors.New("unsupported role")
		}
		if provider.Kind == "anthropic" {
			if m.Role == "system" {
				if conversation {
					return errors.New("system messages must be leading")
				}
			} else {
				if !conversation && m.Role != "user" {
					return errors.New("first conversation message must be user")
				}
				conversation = true
			}
		}
	}
	if provider.Kind == "anthropic" {
		if !conversation || r.Messages[len(r.Messages)-1].Role != "user" {
			return errors.New("assistant prefilling unsupported")
		}
		if r.Temperature != nil && r.TopP != nil {
			return fmt.Errorf("simultaneous temperature and top_p unsupported")
		}
	}
	return nil
}
