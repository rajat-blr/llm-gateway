package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

var errProtocol = errors.New("invalid upstream protocol")

type idleConn struct {
	net.Conn
	idle time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.idle)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}
func newTransport(c Config) *http.Transport {
	dialer := &net.Dialer{Timeout: time.Duration(c.IdleTimeout), KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &idleConn{conn, time.Duration(c.IdleTimeout)}, nil
		},
		MaxIdleConns: c.MaxConcurrent, MaxIdleConnsPerHost: c.MaxConcurrent,
		MaxConnsPerHost: c.MaxConcurrent, IdleConnTimeout: 30 * time.Second,
		ResponseHeaderTimeout: time.Duration(c.IdleTimeout), TLSHandshakeTimeout: time.Duration(c.IdleTimeout),
		MaxResponseHeaderBytes: 32 << 10,
		// HTTP/1.1 keeps each stream's read deadline independent.
		ForceAttemptHTTP2: false,
	}
}
func upstreamRequest(ctx context.Context, r Request, route Target, p ProviderConfig, key string) (*http.Request, error) {
	var body any
	endpoint := "/chat/completions"
	if p.Kind == "openai" {
		r.Model = route.Model
		if r.Stream {
			r.StreamOptions = &StreamOptions{true}
		}
		body = r
	} else {
		endpoint = "/messages"
		a := map[string]any{"model": route.Model, "max_tokens": *r.MaxTokens, "stream": r.Stream}
		var system []map[string]string
		messages := make([]Message, 0, len(r.Messages))
		for _, m := range r.Messages {
			if m.Role == "system" {
				system = append(system, map[string]string{"type": "text", "text": m.Content})
			} else {
				messages = append(messages, m)
			}
		}
		a["messages"] = messages
		if len(system) > 0 {
			a["system"] = system
		}
		if r.Temperature != nil {
			a["temperature"] = *r.Temperature
		}
		if r.TopP != nil {
			a["top_p"] = *r.TopP
		}
		if r.Stop != nil {
			a["stop_sequences"] = r.Stop
		}
		body = a
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.BaseURL, "/")+endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.Stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	if p.Kind == "openai" {
		req.Header.Set("Authorization", "Bearer "+key)
	} else {
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	return req, nil
}
func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, errProtocol
	}
	return b, nil
}
func finishReason(kind, reason string) (string, error) {
	if kind == "anthropic" {
		switch reason {
		case "end_turn", "stop_sequence":
			return "stop", nil
		case "max_tokens":
			return "length", nil
		default:
			return "", errProtocol
		}
	}
	switch reason {
	case "stop", "length", "content_filter":
		return reason, nil
	default:
		return "", errProtocol
	}
}
func decodeCompletion(b []byte, kind string) (Message, string, *Usage, error) {
	var m Message
	if kind == "openai" {
		var v struct {
			Error   json.RawMessage `json:"error"`
			Choices []struct {
				Index   int `json:"index"`
				Message struct {
					Role         string          `json:"role"`
					Content      *string         `json:"content"`
					ToolCalls    json.RawMessage `json:"tool_calls"`
					FunctionCall json.RawMessage `json:"function_call"`
					Refusal      *string         `json:"refusal"`
				} `json:"message"`
				Finish string `json:"finish_reason"`
			} `json:"choices"`
			Usage *reportedUsage `json:"usage"`
		}
		if json.Unmarshal(b, &v) != nil || len(v.Error) > 0 || len(v.Choices) != 1 {
			return m, "", nil, errProtocol
		}
		c := v.Choices[0]
		if c.Index != 0 || c.Message.Role != "assistant" || c.Message.Content == nil || present(c.Message.ToolCalls) || present(c.Message.FunctionCall) || c.Message.Refusal != nil {
			return m, "", nil, errProtocol
		}
		reason, err := finishReason(kind, c.Finish)
		usage, usageErr := v.Usage.normalized()
		if usageErr != nil {
			return m, "", nil, errProtocol
		}
		return Message{"assistant", *c.Message.Content}, reason, usage, err
	}
	var v struct {
		Type    string `json:"type"`
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Stop  string          `json:"stop_reason"`
		Usage *anthropicUsage `json:"usage"`
	}
	if json.Unmarshal(b, &v) != nil || v.Type != "message" || v.Role != "assistant" || len(v.Content) == 0 {
		return m, "", nil, errProtocol
	}
	var text strings.Builder
	for _, block := range v.Content {
		if block.Type != "text" {
			return m, "", nil, errProtocol
		}
		text.WriteString(block.Text)
	}
	reason, err := finishReason(kind, v.Stop)
	usage, usageErr := v.Usage.normalized()
	if usageErr != nil {
		return m, "", nil, errProtocol
	}

	return Message{"assistant", text.String()}, reason, usage, err
}
func present(b json.RawMessage) bool { return len(b) > 0 && string(b) != "null" }
