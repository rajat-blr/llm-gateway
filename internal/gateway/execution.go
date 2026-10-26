package gateway

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/rajat-blr/llm-gateway/internal/limits"
	"github.com/rajat-blr/llm-gateway/internal/storage"
)

type completionValue struct {
	Message  Message `json:"message"`
	Finish   string  `json:"finish"`
	Usage    *Usage  `json:"usage,omitempty"`
	Provider string  `json:"provider"`
}
type inferenceResult struct {
	value         completionValue
	attempts      []storage.Attempt
	remaining     int64
	status        int
	retryAfter    time.Duration
	code, outcome string
	streamHandled bool
}
type streamConsumer func(*Stream, Event, string, int64) string

func failureResult(ctx context.Context) inferenceResult {
	status, code := upstreamError(ctx)
	return inferenceResult{status: status, code: code, outcome: code}
}
func unavailableResult() inferenceResult {
	return inferenceResult{status: 503, code: "dependency_unavailable", outcome: "dependency_unavailable"}
}
func (s *Server) runRoute(ctx context.Context, snapshot *routingSnapshot, key storage.Key, request Request, id string, initial *limits.Reservation, consume streamConsumer) (result inferenceResult) {
	route := snapshot.config.Routes[request.Model]
	defer func() {
		if initial != nil {
			if s.release(*initial) != nil && !result.streamHandled {
				result.status = 503
				result.code = "dependency_unavailable"
				result.outcome = result.code
			}
		}
	}()
	releaseInitial := func() error {
		if initial == nil {
			return nil
		}
		reservation := *initial
		initial = nil
		return s.release(reservation)
	}
	lastFailure := false
	attempted := 0
	for position, target := range route.Targets {
		if ctx.Err() != nil {
			failure := failureResult(ctx)
			result.status = failure.status
			result.code = failure.code
			result.outcome = failure.outcome
			return
		}
		provider := snapshot.config.Providers[target.Provider]
		breaker := snapshot.breakers[breakerIdentity(provider, s.secrets[target.Provider], target.Model)]
		ticket, allowed := breaker.acquire()
		if !allowed {
			if position == 0 && releaseInitial() != nil {
				return unavailableResult()
			}
			continue
		}
		var reservation limits.Reservation
		var err error
		if position == 0 && initial != nil {
			reservation = *initial
			initial = nil
		} else {
			reservation, err = s.limiter.Reserve(ctx, key.ID, id+"-"+strconv.Itoa(position), estimateInput(request, provider.Kind)+int64(*request.MaxTokens), limits.Policy{Capacity: key.Capacity, RefillPerSecond: key.RefillPerSecond})
		}
		if err != nil {
			breaker.complete(ticket, breakerNeutral)
			if ctx.Err() != nil {
				failure := failureResult(ctx)
				result.status = failure.status
				result.code = failure.code
				result.outcome = failure.outcome
				return
			}
			var denied *limits.Denied
			if errors.As(err, &denied) {
				result.status = 429
				result.code = "rate_limit_exceeded"
				result.outcome = result.code
				result.remaining = denied.Remaining
				result.value.Provider = ""
				result.retryAfter = denied.RetryAfter
			} else {
				result.status = 503
				result.code = "dependency_unavailable"
				result.outcome = result.code
			}
			return
		}
		if ctx.Err() != nil {
			breaker.complete(ticket, breakerNeutral)
			s.release(reservation)
			failure := failureResult(ctx)
			result.status = failure.status
			result.code = failure.code
			result.outcome = failure.outcome
			return
		}
		attempted++
		start := time.Now()
		attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(target.AttemptTimeout))
		response := s.execute(attemptCtx, request, target, provider)
		if response.err != nil {
			cancel()
			classification := breakerNeutral
			if response.retry && ctx.Err() == nil {
				classification = breakerFailure
			}
			breaker.complete(ticket, classification)
			accountingErr := s.settle(id, target.Provider, attempted, reservation, nil)
			result.attempts = append(result.attempts, attemptRecord(attempted, target.Provider, start, "upstream_failure", reservation, nil, accountingErr))
			slog.Info("upstream attempt", "request_id", id, "alias", request.Model, "provider", target.Provider, "attempt", attempted, "duration_ms", time.Since(start).Milliseconds(), "outcome", "failed", "transient", response.retry, "config_generation", snapshot.generation)
			result.value.Provider = target.Provider
			lastFailure = true
			if ctx.Err() != nil {
				failure := failureResult(ctx)
				result.status = failure.status
				result.code = failure.code
				result.outcome = failure.outcome
				return
			}
			if accountingErr != nil {
				result.status = 503
				result.code = "dependency_unavailable"
				result.outcome = result.code
				return
			}
			if !response.retry || position == len(route.Targets)-1 || !s.waitRetry(ctx, response.retryAfter) {
				failure := failureResult(ctx)
				result.status = failure.status
				result.code = failure.code
				result.outcome = failure.outcome
				return
			}
			continue
		}
		result.remaining = reservation.Remaining
		result.value.Provider = target.Provider
		var usage *Usage
		if response.stream != nil {
			result.streamHandled = true
			result.outcome = consume(response.stream, response.first, target.Provider, reservation.Remaining)
			response.stream.Close()
			cancel()
			classification := breakerNeutral
			if response.stream.done {
				usage = response.stream.usage
				classification = breakerSuccess
			} else if ctx.Err() == nil && transient(response.stream.lastErr) {
				classification = breakerFailure
			}
			breaker.complete(ticket, classification)
		} else {
			cancel()
			breaker.complete(ticket, breakerSuccess)
			usage = response.usage
			result.outcome = "success"
			result.value = completionValue{Message: response.message, Finish: response.reason, Usage: usage, Provider: target.Provider}
		}
		accountingErr := s.settle(id, target.Provider, attempted, reservation, usage)
		result.attempts = append(result.attempts, attemptRecord(attempted, target.Provider, start, result.outcome, reservation, usage, accountingErr))
		if response.stream != nil {
			result.value.Usage = usage
		} else if accountingErr != nil {
			result.status = 503
			result.code = "dependency_unavailable"
			result.outcome = result.code
		}
		slog.Info("upstream attempt", "request_id", id, "alias", request.Model, "provider", target.Provider, "attempt", attempted, "duration_ms", time.Since(start).Milliseconds(), "outcome", result.outcome, "config_generation", snapshot.generation)
		return
	}
	if lastFailure {
		failure := failureResult(ctx)
		result.status = failure.status
		result.code = failure.code
		result.outcome = failure.outcome
	} else {
		result.status = 503
		result.code = "all_providers_unavailable"
		result.outcome = result.code
	}
	return
}
