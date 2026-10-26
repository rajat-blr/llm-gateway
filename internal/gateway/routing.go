package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rajat-blr/llm-gateway/internal/limits"
	"github.com/rajat-blr/llm-gateway/internal/storage"
)

type attemptResponse struct {
	stream     *Stream
	first      Event
	message    Message
	reason     string
	usage      *Usage
	err        error
	retry      bool
	retryAfter time.Duration
}

func transient(err error) bool {
	if err == nil || errors.Is(err, errProtocol) {
		return false
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
func (s *Server) execute(ctx context.Context, request Request, target Target, p ProviderConfig) attemptResponse {
	fail := func(err error) attemptResponse { return attemptResponse{err: err, retry: transient(err)} }
	upstream, err := upstreamRequest(ctx, request, target, p, s.secrets[target.Provider])
	if err != nil {
		return fail(err)
	}
	response, err := s.client.Do(upstream)
	if err != nil {
		return fail(err)
	}
	owned := false
	defer func() {
		if !owned {
			response.Body.Close()
		}
	}()
	if response.StatusCode != 200 {
		return attemptResponse{err: errProtocol, retry: response.StatusCode == 429 || response.StatusCode >= 500 && response.StatusCode <= 599, retryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now())}
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return fail(errProtocol)
	}
	if request.Stream {
		if media != "text/event-stream" {
			return fail(errProtocol)
		}
		stream := newStream(response.Body, p.Kind, s.cfg.MaxEventBytes, s.cfg.MaxOutputBytes)
		first, err := stream.Next(ctx)
		if err != nil {
			return fail(err)
		}
		owned = true
		return attemptResponse{stream: stream, first: first}
	}
	if media != "application/json" {
		return fail(errProtocol)
	}
	b, err := readBounded(response.Body, s.cfg.MaxOutputBytes)
	if err != nil {
		return fail(err)
	}
	message, reason, usage, err := decodeCompletion(b, p.Kind)
	if err != nil {
		return fail(err)
	}
	return attemptResponse{message: message, reason: reason, usage: usage}
}
func breakerIdentity(p ProviderConfig, key, model string) string {
	sum := sha256.Sum256([]byte(p.Kind + "\x00" + strings.TrimRight(p.BaseURL, "/") + "\x00" + key + "\x00" + model))
	return hex.EncodeToString(sum[:])
}

// This byte-based estimate is intentionally approximate, not a spending cap.
func estimateInput(request Request, kind string) int64 {
	n := int64(2)
	overhead := int64(4)
	if kind == "anthropic" {
		overhead = 6
		n = 4
	}
	for _, m := range request.Messages {
		n += overhead + int64((len(m.Content)+3)/4)
	}
	return n
}
func (s *Server) settle(id, provider string, attempt int, res limits.Reservation, usage *Usage) error {
	var actual *int64
	source := "unknown"
	if usage != nil {
		n := int64(usage.TotalTokens)
		actual = &n
		source = "provider_reported"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.Redis.Timeout))
	defer cancel()
	err := s.limiter.Reconcile(ctx, res, actual)
	if err != nil {
		s.accountingErrors.Add(1)
		slog.Error("accounting reconciliation failed", "request_id", id, "provider", provider, "attempt", attempt)
	}
	args := []any{"request_id", id, "provider", provider, "attempt", attempt, "reserved_tokens", res.Amount, "usage_source", source}
	if actual != nil {
		args = append(args, "upstream_total_tokens", *actual)
	}
	slog.Info("attempt accounting", args...)
	return err
}
func (s *Server) release(res limits.Reservation) error {
	zero := int64(0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.cfg.Redis.Timeout))
	defer cancel()
	err := s.limiter.Reconcile(ctx, res, &zero)
	if err != nil {
		s.accountingErrors.Add(1)
		slog.Error("unused reservation release failed")
	}
	return err
}
func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
func (s *Server) waitRetry(ctx context.Context, retryAfter time.Duration) bool {
	delay := time.Duration(s.cfg.BackoffBase)/2 + time.Duration(rand.Int64N(int64(s.cfg.BackoffBase)/2+1))
	if delay > time.Duration(s.cfg.BackoffMax) {
		delay = time.Duration(s.cfg.BackoffMax)
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	if deadline, ok := ctx.Deadline(); ok && delay >= time.Until(deadline) {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func attemptRecord(number int, provider string, start time.Time, outcome string, res limits.Reservation, usage *Usage, accountingErr error) storage.Attempt {
	a := storage.Attempt{Number: number, Provider: provider, StartedMS: start.UnixMilli(), DurationMS: time.Since(start).Milliseconds(), Outcome: outcome, ReservedTokens: res.Amount, AccountingError: accountingErr != nil}
	if usage != nil {
		a.InputTokens = &usage.PromptTokens
		a.OutputTokens = &usage.CompletionTokens
	}
	return a
}
func keyAllows(key storage.Key, alias string) bool {
	for _, allowed := range key.Aliases {
		if allowed == alias {
			return true
		}
	}
	return false
}
