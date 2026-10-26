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
	"strconv"

	"github.com/rajat-blr/llm-gateway/internal/limits"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rajat-blr/llm-gateway/internal/storage"
	"time"
)

type Server struct {
	cfg              Config
	adminHash        [32]byte
	identities       *storage.Store
	usage            *storage.Worker
	shutdownOnce     sync.Once
	shutdownErr      error
	secrets          map[string]string
	transport        *http.Transport
	client           *http.Client
	slots            chan struct{}
	streams          chan struct{}
	ready            atomic.Bool
	limiter          limits.Store
	breakers         map[string]*breaker
	accountingErrors atomic.Uint64
}

func New(c Config) (*Server, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Redis.PasswordEnv != "" && os.Getenv(c.Redis.PasswordEnv) == "" {
		return nil, errors.New("Redis password environment variable is required")
	}
	limiter, err := limits.New(limits.Options{Address: c.Redis.Address, Password: os.Getenv(c.Redis.PasswordEnv), DB: c.Redis.DB, Prefix: c.Redis.Prefix, Capacity: c.Limit.Capacity, RefillPerSecond: c.Limit.RefillPerSecond, Timeout: time.Duration(c.Redis.Timeout), ReservationTTL: time.Duration(c.Redis.ReservationTTL)})
	if err != nil {
		return nil, err
	}
	server, err := newServer(c, limiter)
	if err != nil {
		limiter.Close()
	}
	return server, err
}
func newServer(c Config, limiter limits.Store) (*Server, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	key := os.Getenv(c.KeyEnv)
	admin := os.Getenv(c.AdminTokenEnv)
	if admin == "" || admin == key {
		return nil, errors.New("a separate admin token environment variable is required")
	}
	providers := make(map[string]ProviderConfig, len(c.Providers))
	for name, p := range c.Providers {
		providers[name] = p
	}
	c.Providers = providers
	routes := make(map[string]Route, len(c.Routes))
	for name, r := range c.Routes {
		r.Targets = append([]Target(nil), r.Targets...)
		routes[name] = r
	}
	c.Routes = routes
	s := &Server{cfg: c, adminHash: sha256.Sum256([]byte(admin)), secrets: map[string]string{}, slots: make(chan struct{}, c.MaxConcurrent), streams: make(chan struct{}, c.MaxStreams)}
	for name, p := range c.Providers {
		secret := os.Getenv(p.KeyEnv)
		if secret == "" {
			return nil, fmt.Errorf("credential environment variable required for %q", name)
		}
		s.secrets[name] = secret
	}
	s.limiter = limiter
	s.breakers = map[string]*breaker{}
	for _, route := range c.Routes {
		seen := map[string]bool{}
		for _, target := range route.Targets {
			identity := breakerIdentity(c.Providers[target.Provider], s.secrets[target.Provider], target.Model)
			if seen[identity] {
				return nil, errors.New("route repeats the same upstream credential and model")
			}
			seen[identity] = true
			if s.breakers[identity] == nil {
				s.breakers[identity] = newBreaker(c.Breaker, time.Now)
			}
		}
	}
	identities, err := storage.Open(storage.Options{Path: c.SQLite.Path, BusyTimeout: time.Duration(c.SQLite.BusyTimeout), Timeout: time.Duration(c.SQLite.Timeout)})
	if err != nil {
		return nil, err
	}
	aliases := make([]string, 0, len(c.Routes))
	maxOutput := 1
	for alias, route := range c.Routes {
		aliases = append(aliases, alias)
		maxOutput = max(maxOutput, route.MaxOutputTokens)
	}
	sort.Strings(aliases)
	if err = identities.EnsureBootstrap(context.Background(), key, storage.KeySpec{Name: "bootstrap", Aliases: aliases, Capacity: c.Limit.Capacity, RefillPerSecond: c.Limit.RefillPerSecond, MaxOutputTokens: maxOutput}); err != nil {
		identities.Close()
		return nil, err
	}
	s.identities = identities
	s.usage = storage.NewWorker(identities, c.SQLite.UsageQueue)
	s.transport = newTransport(c)
	s.client = &http.Client{Transport: s.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	s.ready.Store(true)
	return s, nil
}
func (s *Server) StopAdmission() { s.ready.Store(false) }
func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.ShutdownTimeout))
	defer cancel()
	_ = s.Shutdown(ctx)
}
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.StopAdmission()
		s.transport.CloseIdleConnections()
		_ = s.limiter.Close()
		s.shutdownErr = s.usage.Drain(ctx)
		if s.shutdownErr == nil {
			s.shutdownErr = s.identities.Close()
		} else {
			stats := s.usage.Stats()
			slog.Warn("usage drain deadline exceeded", "queued_records", stats.QueueDepth, "in_flight", stats.InFlight)
			go func() { s.usage.Wait(); s.identities.Close() }()
		}
	})
	return s.shutdownErr
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if serveDemo(w, r) {
		return
	}
	if r.Method == http.MethodGet && (r.URL.Path == "/healthz" || r.URL.Path == "/readyz") {
		if r.URL.Path == "/readyz" && (!s.ready.Load() || s.limiter.Ping(r.Context()) != nil || s.identities.Ping(r.Context()) != nil) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	requestStart := time.Now()
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
	if strings.HasPrefix(r.URL.Path, "/admin/") {
		s.serveAdmin(w, r)
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	hash := sha256.Sum256([]byte(token))
	if !ok || token == "" || subtle.ConstantTimeCompare(hash[:], s.adminHash[:]) == 1 {
		s.writeError(w, 401, "invalid_api_key")
		return
	}
	identity, err := s.identities.Authenticate(r.Context(), token)
	if err != nil {
		if r.Context().Err() != nil {
			status, code := upstreamError(r.Context())
			s.writeError(w, status, code)
			return
		}
		if errors.Is(err, storage.ErrInvalidKey) {
			s.writeError(w, 401, "invalid_api_key")
		} else {
			s.writeError(w, 503, "dependency_unavailable")
		}
		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		names := make([]string, 0, len(s.cfg.Routes))
		for _, name := range identity.Aliases {
			if _, ok := s.cfg.Routes[name]; ok {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		data := make([]map[string]any, 0, len(names))
		for _, name := range names {
			data = append(data, map[string]any{"id": name, "object": "model", "created": 0, "owned_by": "gateway"})
		}
		writeJSON(w, 200, map[string]any{"object": "list", "data": data})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		s.chat(w, r, id, identity, requestStart)
	default:
		s.writeError(w, 404, "invalid_request")
	}
}
func (s *Server) chat(w http.ResponseWriter, r *http.Request, id string, identity storage.Key, start time.Time) {
	alias := ""
	provider := ""
	outcome := "invalid_request"
	record := storage.Record{ID: id, KeyID: identity.ID, StartedMS: start.UnixMilli()}
	defer func() {
		record.Alias = alias
		record.Provider = provider
		record.Outcome = outcome
		record.DurationMS = time.Since(start).Milliseconds()
		if errors.Is(r.Context().Err(), context.Canceled) {
			record.Outcome = "canceled"
		}
		if outcome != "success" {
			record.ClientInput = nil
			record.ClientOutput = nil
		}
		s.usage.Enqueue(record)
	}()
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
	if !ok || !keyAllows(identity, request.Model) {
		outcome = "model_not_allowed"
		s.writeError(w, 403, "model_not_allowed")
		return
	}
	alias = request.Model
	route.MaxOutputTokens = min(route.MaxOutputTokens, identity.MaxOutputTokens)
	for _, target := range route.Targets {
		if request.Validate(route, s.cfg.Providers[target.Provider]) != nil {
			s.writeError(w, 400, "invalid_request")
			return
		}
	}
	fail := func(status int, code string) { outcome = code; s.writeError(w, status, code) }
	contextFailure := func() { status, code := upstreamError(r.Context()); fail(status, code) }
	if err := acquire(r.Context(), s.slots); err != nil {
		contextFailure()
		return
	}
	defer func() { <-s.slots }()
	if request.Stream {
		if err := acquire(r.Context(), s.streams); err != nil {
			contextFailure()
			return
		}
		defer func() { <-s.streams }()
	}
	attempted := 0
	lastFailure := false
	for position, target := range route.Targets {
		if r.Context().Err() != nil {
			contextFailure()
			return
		}
		p := s.cfg.Providers[target.Provider]
		breaker := s.breakers[breakerIdentity(p, s.secrets[target.Provider], target.Model)]
		ticket, ok := breaker.acquire()
		if !ok {
			continue
		}
		amount := estimateInput(request, p.Kind) + int64(*request.MaxTokens)
		reservation, err := s.limiter.Reserve(r.Context(), identity.ID, id+"-"+strconv.Itoa(position), amount, limits.Policy{Capacity: identity.Capacity, RefillPerSecond: identity.RefillPerSecond})
		if err != nil {
			breaker.complete(ticket, breakerNeutral)
			if r.Context().Err() != nil {
				contextFailure()
				return
			}
			var denied *limits.Denied
			if errors.As(err, &denied) {
				w.Header().Set("X-RateLimit-Remaining-Tokens", strconv.FormatInt(max(int64(0), denied.Remaining), 10))
				if denied.RetryAfter > 0 {
					w.Header().Set("Retry-After", strconv.FormatInt(int64((denied.RetryAfter+time.Second-1)/time.Second), 10))
				}
				fail(429, "rate_limit_exceeded")
			} else {
				fail(503, "dependency_unavailable")
			}
			return
		}
		if r.Context().Err() != nil {
			breaker.complete(ticket, breakerNeutral)
			s.release(reservation)
			contextFailure()
			return
		}
		attempted++
		provider = target.Provider
		attemptStart := time.Now()
		attemptCtx, cancelAttempt := context.WithTimeout(r.Context(), time.Duration(target.AttemptTimeout))
		result := s.execute(attemptCtx, request, target, p)
		if result.err != nil {
			cancelAttempt()
			classification := breakerNeutral
			if result.retry && r.Context().Err() == nil {
				classification = breakerFailure
			}
			breaker.complete(ticket, classification)
			slog.Info("upstream attempt", "request_id", id, "alias", alias, "provider", provider, "attempt", attempted, "duration_ms", time.Since(attemptStart).Milliseconds(), "outcome", "failed", "transient", result.retry)
			accountingErr := s.settle(id, provider, attempted, reservation, nil)
			record.Attempts = append(record.Attempts, attemptRecord(attempted, provider, attemptStart, "upstream_failure", reservation, nil, accountingErr))
			if r.Context().Err() != nil {
				contextFailure()
				return
			}
			if accountingErr != nil {
				fail(503, "dependency_unavailable")
				return
			}
			lastFailure = true
			if !result.retry || position == len(route.Targets)-1 {
				contextFailure()
				return
			}
			if !s.waitRetry(r.Context(), result.retryAfter) {
				contextFailure()
				return
			}
			continue
		}
		w.Header().Set("X-Gateway-Provider", provider)
		w.Header().Set("X-Cache", "BYPASS")
		w.Header().Set("X-RateLimit-Remaining-Tokens", strconv.FormatInt(max(int64(0), reservation.Remaining), 10))
		if result.stream != nil {
			// From this point the response belongs to this provider, including errors.
			outcome = s.serveStream(w, r, result.stream, result.first, id, alias, request.StreamOptions != nil && request.StreamOptions.IncludeUsage)
			result.stream.Close()
			cancelAttempt()
			classification := breakerNeutral
			var usage *Usage
			if result.stream.done {
				usage = result.stream.usage
				classification = breakerSuccess
			} else if r.Context().Err() == nil && transient(result.stream.lastErr) {
				classification = breakerFailure
			}
			breaker.complete(ticket, classification)
			accountingErr := s.settle(id, provider, attempted, reservation, usage)
			record.Attempts = append(record.Attempts, attemptRecord(attempted, provider, attemptStart, outcome, reservation, usage, accountingErr))
			if usage != nil {
				record.ClientInput = &usage.PromptTokens
				record.ClientOutput = &usage.CompletionTokens
			}
			slog.Info("upstream attempt", "request_id", id, "alias", alias, "provider", provider, "attempt", attempted, "duration_ms", time.Since(attemptStart).Milliseconds(), "outcome", outcome)
			return
		}
		cancelAttempt()
		breaker.complete(ticket, breakerSuccess)
		accountingErr := s.settle(id, provider, attempted, reservation, result.usage)
		record.Attempts = append(record.Attempts, attemptRecord(attempted, provider, attemptStart, "success", reservation, result.usage, accountingErr))
		if result.usage != nil {
			record.ClientInput = &result.usage.PromptTokens
			record.ClientOutput = &result.usage.CompletionTokens
		}
		if accountingErr != nil {
			w.Header().Del("X-Gateway-Provider")
			w.Header().Del("X-Cache")
			fail(503, "dependency_unavailable")
			return
		}
		slog.Info("upstream attempt", "request_id", id, "alias", alias, "provider", provider, "attempt", attempted, "duration_ms", time.Since(attemptStart).Milliseconds(), "outcome", "success")
		c := Completion{ID: id, Object: "chat.completion", Created: start.Unix(), Model: alias, Choices: []Choice{{Index: 0, Message: &result.message, FinishReason: &result.reason}}, Usage: result.usage}
		_ = http.NewResponseController(w).SetWriteDeadline(writeDeadline(r.Context(), time.Duration(s.cfg.WriteTimeout)))
		if writeJSON(w, 200, c) != nil {
			outcome = "downstream_failure"
		} else {
			outcome = "success"
		}
		return
	}
	if lastFailure {
		contextFailure()
	} else {
		fail(503, "all_providers_unavailable")
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
