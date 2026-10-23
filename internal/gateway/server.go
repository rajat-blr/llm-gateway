package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type Server struct {
	cfg       Config
	key       [32]byte
	secrets   map[string]string
	transport *http.Transport
	client    *http.Client
	slots     chan struct{}
	streams   chan struct{}
	ready     atomic.Bool
}

func New(c Config) (*Server, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	key := os.Getenv(c.KeyEnv)
	if key == "" {
		return nil, errors.New("gateway key environment variable is required")
	}
	s := &Server{cfg: c, key: sha256.Sum256([]byte(key)), secrets: map[string]string{}, slots: make(chan struct{}, c.MaxConcurrent), streams: make(chan struct{}, c.MaxStreams)}
	for name, p := range c.Providers {
		secret := os.Getenv(p.KeyEnv)
		if secret == "" {
			return nil, fmt.Errorf("credential environment variable required for %q", name)
		}
		s.secrets[name] = secret
	}
	s.transport = newTransport(c)
	s.client = &http.Client{Transport: s.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s.ready.Store(true)
	return s, nil
}
func (s *Server) StopAdmission() { s.ready.Store(false) }
func (s *Server) Close()         { s.transport.CloseIdleConnections() }
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if serveDemo(w, r) {
		return
	}
	if r.Method == http.MethodGet && (r.URL.Path == "/healthz" || r.URL.Path == "/readyz") {
		if r.URL.Path == "/readyz" && !s.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	id := requestID()
	w.Header().Set("X-Request-ID", id)
	rc := http.NewResponseController(w)
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.cfg.RequestTimeout))
	defer cancel()
	r = r.WithContext(ctx)
	deadline, _ := ctx.Deadline()
	_ = rc.SetReadDeadline(deadline)
	defer func() { _ = rc.SetReadDeadline(time.Time{}) }()
	_ = rc.SetWriteDeadline(minTime(deadline, time.Now().Add(time.Duration(s.cfg.WriteTimeout))))
	if !s.ready.Load() {
		s.writeError(w, 503, "dependency_unavailable")
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	hash := sha256.Sum256([]byte(token))
	if !ok || subtle.ConstantTimeCompare(hash[:], s.key[:]) != 1 {
		s.writeError(w, 401, "invalid_api_key")
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		names := make([]string, 0, len(s.cfg.Routes))
		for name := range s.cfg.Routes {
			names = append(names, name)
		}
		sort.Strings(names)
		data := make([]map[string]any, 0, len(names))
		for _, name := range names {
			data = append(data, map[string]any{"id": name, "object": "model", "created": 0, "owned_by": "gateway"})
		}
		writeJSON(w, 200, map[string]any{"object": "list", "data": data})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		s.chat(w, r, id)
	default:
		s.writeError(w, 404, "invalid_request")
	}
}
func (s *Server) chat(w http.ResponseWriter, r *http.Request, id string) {
	start := time.Now()
	alias := ""
	provider := ""
	outcome := "invalid_request"
	defer func() {
		slog.Info("request", "request_id", id, "alias", alias, "provider", provider, "duration_ms", time.Since(start).Milliseconds(), "outcome", outcome, "cache", "BYPASS")
	}()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		s.writeError(w, 400, "invalid_request")
		return
	}
	b, err := readBounded(r.Body, s.cfg.MaxBodyBytes)
	if err != nil {
		s.writeError(w, 400, "invalid_request")
		return
	}
	var request Request
	if StrictJSON(b, &request) != nil {
		s.writeError(w, 400, "invalid_request")
		return
	}
	route, ok := s.cfg.Routes[request.Model]
	if !ok {
		outcome = "model_not_allowed"
		s.writeError(w, 403, "model_not_allowed")
		return
	}
	alias = request.Model
	p := s.cfg.Providers[route.Provider]
	if request.Validate(route, p) != nil {
		s.writeError(w, 400, "invalid_request")
		return
	}
	fail := func() { status, code := upstreamError(r.Context()); outcome = code; s.writeError(w, status, code) }
	if err := acquire(r.Context(), s.slots); err != nil {
		fail()
		return
	}
	defer func() { <-s.slots }()
	if request.Stream {
		if err := acquire(r.Context(), s.streams); err != nil {
			fail()
			return
		}
		defer func() { <-s.streams }()
	}
	upstream, err := upstreamRequest(r.Context(), request, route, p, s.secrets[route.Provider])
	if err != nil {
		fail()
		return
	}
	provider = route.Provider
	response, err := s.client.Do(upstream)
	if err != nil {
		fail()
		return
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		fail()
		return
	}
	media, _, err = mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		fail()
		return
	}
	if request.Stream {
		if media != "text/event-stream" {
			fail()
			return
		}
		stream := newStream(response.Body, p.Kind, s.cfg.MaxEventBytes, s.cfg.MaxOutputBytes)
		defer stream.Close()
		first, err := stream.Next(r.Context())
		if err != nil {
			fail()
			return
		}
		w.Header().Set("X-Gateway-Provider", provider)
		w.Header().Set("X-Cache", "BYPASS")
		outcome = s.serveStream(w, r, stream, first, id, alias, request.StreamOptions != nil && request.StreamOptions.IncludeUsage)
		return
	}
	if media != "application/json" {
		fail()
		return
	}
	b, err = readBounded(response.Body, s.cfg.MaxOutputBytes)
	if err != nil {
		fail()
		return
	}
	message, reason, usage, err := decodeCompletion(b, p.Kind)
	if err != nil {
		fail()
		return
	}
	w.Header().Set("X-Gateway-Provider", provider)
	w.Header().Set("X-Cache", "BYPASS")
	c := Completion{ID: id, Object: "chat.completion", Created: start.Unix(), Model: alias, Choices: []Choice{{Index: 0, Message: &message, FinishReason: &reason}}, Usage: usage}
	_ = http.NewResponseController(w).SetWriteDeadline(writeDeadline(r.Context(), time.Duration(s.cfg.WriteTimeout)))
	if writeJSON(w, 200, c) != nil {
		outcome = "downstream_failure"
	} else {
		outcome = "success"
	}
}
func (s *Server) serveStream(w http.ResponseWriter, r *http.Request, stream *Stream, event Event, id, alias string, includeUsage bool) string {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	created := time.Now().Unix()
	var written int64
	committed := false
	failure := func(code string) string {
		if !committed {
			status := 502
			if code == "deadline_exceeded" {
				status = 504
			}
			w.Header().Del("X-Gateway-Provider")
			w.Header().Del("X-Cache")
			s.writeError(w, status, code)
		} else {
			// A fixed-size best-effort error can exceed the data-byte budget.
			// It has a separate finite write window and never emits [DONE].
			_ = rc.SetWriteDeadline(time.Now().Add(time.Duration(s.cfg.WriteTimeout)))
			b, _ := json.Marshal(apiError(code))
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err == nil {
				_ = rc.Flush()
			}
		}
		return code
	}
	send := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		written += int64(len(b) + 8)
		if written > s.cfg.MaxOutputBytes {
			return errProtocol
		}
		if err = rc.SetWriteDeadline(writeDeadline(r.Context(), time.Duration(s.cfg.WriteTimeout))); err != nil {
			return err
		}
		committed = true
		if _, err = fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return err
		}
		return rc.Flush()
	}
	onWriteError := func(err error) string {
		if errors.Is(err, errProtocol) {
			return failure("upstream_failure")
		}
		return "downstream_failure"
	}
	for {
		if event.Done {
			if includeUsage && event.Usage != nil {
				if err := send(Completion{ID: id, Object: "chat.completion.chunk", Created: created, Model: alias, Choices: []Choice{}, Usage: event.Usage}); err != nil {
					return onWriteError(err)
				}
			}
			if written+int64(len("data: [DONE]\n\n")) > s.cfg.MaxOutputBytes {
				return failure("upstream_failure")
			}
			if err := rc.SetWriteDeadline(writeDeadline(r.Context(), time.Duration(s.cfg.WriteTimeout))); err != nil {
				return "downstream_failure"
			}
			if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
				return "downstream_failure"
			}
			if rc.Flush() != nil {
				return "downstream_failure"
			}
			return "success"
		}
		if event.Delta != nil || event.Finish != nil {
			if err := send(Completion{ID: id, Object: "chat.completion.chunk", Created: created, Model: alias, Choices: []Choice{{Index: 0, Delta: event.Delta, FinishReason: event.Finish}}}); err != nil {
				return onWriteError(err)
			}
		}
		var err error
		event, err = stream.Next(r.Context())
		if err != nil {
			_, code := upstreamError(r.Context())
			return failure(code)
		}
	}
}

func acquire(ctx context.Context, slots chan struct{}) error {
	select {
	case slots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-slots
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func upstreamError(ctx context.Context) (int, string) {
	deadline, hasDeadline := ctx.Deadline()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || (hasDeadline && !time.Now().Before(deadline)) {
		return 504, "deadline_exceeded"
	}
	return 502, "upstream_failure"
}
func requestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand unavailable")
	}
	return "chatcmpl-" + hex.EncodeToString(b[:])
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func writeDeadline(ctx context.Context, d time.Duration) time.Time {
	deadline := time.Now().Add(d)
	if overall, ok := ctx.Deadline(); ok {
		deadline = minTime(deadline, overall)
	}
	return deadline
}
func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}
func (s *Server) writeError(w http.ResponseWriter, status int, code string) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Duration(s.cfg.WriteTimeout)))
	_ = writeJSON(w, status, apiError(code))
}
