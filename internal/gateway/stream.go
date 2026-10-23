package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
)

// Stream is owned and pulled by the HTTP handler. Next performs synchronous
// upstream reads; there is no producer goroutine or event queue. Close always
// closes the response body, including on cancellation and downstream failure.
type Stream struct {
	body       io.ReadCloser
	scan       *bufio.Scanner
	maxEvent   int
	kind       string
	finish     bool
	started    bool
	blockOpen  bool
	blockIndex int
	done       bool
	usage      *Usage
	lastErr    error
}
type Event struct {
	Delta  *Delta
	Finish *string
	Usage  *Usage
	Done   bool
}

func newStream(body io.ReadCloser, kind string, maxEvent int, maxBytes int64) *Stream {
	scan := bufio.NewScanner(io.LimitReader(body, maxBytes))
	scan.Buffer(make([]byte, 4096), maxEvent)
	return &Stream{body: body, scan: scan, maxEvent: maxEvent, kind: kind, blockIndex: -1}
}
func (s *Stream) Close() error { return s.body.Close() }
func (s *Stream) readEvent() (string, string, error) {
	var data strings.Builder
	name := ""
	size := 0
	for s.scan.Scan() {
		line := s.scan.Text()
		size += len(line) + 1
		if size > s.maxEvent {
			return "", "", errProtocol
		}
		if line == "" {
			if data.Len() > 0 {
				return name, strings.TrimSuffix(data.String(), "\n"), nil
			}
			name = ""
			size = 0
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			value = ""
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			name = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
	if err := s.scan.Err(); err != nil {
		return "", "", err
	}
	return "", "", io.ErrUnexpectedEOF
}
func (s *Stream) Next(ctx context.Context) (event Event, nextErr error) {
	defer func() {
		if nextErr != nil {
			s.lastErr = nextErr
		}
	}()
	if s.done {
		return Event{}, io.EOF
	}
	for {
		if err := ctx.Err(); err != nil {
			return Event{}, err
		}
		name, data, err := s.readEvent()
		if err != nil {
			return Event{}, err
		}
		var event Event
		var emit bool
		if s.kind == "openai" {
			event, emit, err = s.openAI(data)
		} else {
			event, emit, err = s.anthropic(name, data)
		}
		if err != nil {
			return Event{}, err
		}
		if emit {
			return event, nil
		}
	}
}
func (s *Stream) openAI(data string) (Event, bool, error) {
	if data == "[DONE]" {
		if !s.finish {
			return Event{}, false, errProtocol
		}
		s.done = true
		return Event{Done: true, Usage: s.usage}, true, nil
	}
	var v struct {
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			Index int `json:"index"`
			Delta struct {
				Role         string          `json:"role"`
				Content      *string         `json:"content"`
				ToolCalls    json.RawMessage `json:"tool_calls"`
				FunctionCall json.RawMessage `json:"function_call"`
				Refusal      *string         `json:"refusal"`
			} `json:"delta"`
			Finish *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *reportedUsage `json:"usage"`
	}
	if json.Unmarshal([]byte(data), &v) != nil || present(v.Error) {
		return Event{}, false, errProtocol
	}
	if v.Usage != nil {
		usage, usageErr := v.Usage.normalized()
		if usageErr != nil {
			return Event{}, false, errProtocol
		}
		s.usage = usage
	}
	if len(v.Choices) == 0 {
		if v.Usage == nil || !s.finish {
			return Event{}, false, errProtocol
		}
		return Event{}, false, nil
	}
	if len(v.Choices) != 1 || s.finish {
		return Event{}, false, errProtocol
	}
	c := v.Choices[0]
	if c.Index != 0 || (c.Delta.Role != "" && c.Delta.Role != "assistant") || present(c.Delta.ToolCalls) || present(c.Delta.FunctionCall) || c.Delta.Refusal != nil {
		return Event{}, false, errProtocol
	}
	e := Event{Delta: &Delta{Role: c.Delta.Role}}
	if c.Delta.Content != nil {
		e.Delta.Content = *c.Delta.Content
	}
	if c.Finish != nil {
		reason, err := finishReason("openai", *c.Finish)
		if err != nil {
			return Event{}, false, err
		}
		s.finish = true
		e.Finish = &reason
	}
	return e, true, nil
}
func (s *Stream) anthropic(name, data string) (Event, bool, error) {
	var v struct {
		Type    string `json:"type"`
		Index   *int   `json:"index"`
		Message struct {
			Role  string          `json:"role"`
			Usage *anthropicUsage `json:"usage"`
		} `json:"message"`
		Block struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content_block"`
		Delta struct {
			Type string  `json:"type"`
			Text string  `json:"text"`
			Stop *string `json:"stop_reason"`
		} `json:"delta"`
		Usage *struct {
			Output *int `json:"output_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal([]byte(data), &v) != nil || name != v.Type {
		return Event{}, false, errProtocol
	}
	switch v.Type {
	case "ping":
		return Event{}, false, nil
	case "message_start":
		if s.started || v.Message.Role != "assistant" {
			return Event{}, false, errProtocol
		}
		s.started = true
		var usageErr error
		s.usage, usageErr = v.Message.Usage.normalized()
		if usageErr != nil {
			return Event{}, false, usageErr
		}
		return Event{Delta: &Delta{Role: "assistant"}}, true, nil
	case "content_block_start":
		if !s.started || s.finish || s.blockOpen || v.Index == nil || *v.Index != s.blockIndex+1 || v.Block.Type != "text" {
			return Event{}, false, errProtocol
		}
		s.blockIndex = *v.Index
		s.blockOpen = true
		return Event{Delta: &Delta{Content: v.Block.Text}}, true, nil
	case "content_block_delta":
		if !s.blockOpen || v.Index == nil || *v.Index != s.blockIndex || v.Delta.Type != "text_delta" {
			return Event{}, false, errProtocol
		}
		return Event{Delta: &Delta{Content: v.Delta.Text}}, true, nil
	case "content_block_stop":
		if !s.blockOpen || v.Index == nil || *v.Index != s.blockIndex {
			return Event{}, false, errProtocol
		}
		s.blockOpen = false
		return Event{}, false, nil
	case "message_delta":
		if !s.started || s.blockOpen || s.finish || v.Delta.Stop == nil {
			return Event{}, false, errProtocol
		}
		reason, err := finishReason("anthropic", *v.Delta.Stop)
		if err != nil {
			return Event{}, false, err
		}
		s.finish = true
		if v.Usage == nil || v.Usage.Output == nil {
			s.usage = nil
		} else {
			if *v.Usage.Output < 0 || *v.Usage.Output > 1_000_000_000 {
				return Event{}, false, errProtocol
			}
			if s.usage != nil {
				s.usage.CompletionTokens = *v.Usage.Output
				s.usage.TotalTokens = s.usage.PromptTokens + *v.Usage.Output
				if !s.usage.valid() {
					return Event{}, false, errProtocol
				}
			}
		}
		return Event{Delta: &Delta{}, Finish: &reason}, true, nil
	case "message_stop":
		if !s.finish || s.blockOpen {
			return Event{}, false, errProtocol
		}
		s.done = true
		return Event{Done: true, Usage: s.usage}, true, nil
	default:
		return Event{}, false, errProtocol
	}
}
