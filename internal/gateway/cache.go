package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/rajat-blr/llm-gateway/internal/limits"
	"github.com/rajat-blr/llm-gateway/internal/storage"
)

type reuseCounters struct{ hits, misses, coalesced, readErrors, writeErrors, oversized atomic.Uint64 }

func (s *Server) reuseStatus() map[string]any {
	active, waiters := s.flights.stats()
	return map[string]any{"hits": s.reuseCounters.hits.Load(), "misses": s.reuseCounters.misses.Load(), "coalesced": s.reuseCounters.coalesced.Load(), "read_errors": s.reuseCounters.readErrors.Load(), "write_errors": s.reuseCounters.writeErrors.Load(), "oversized_entries": s.reuseCounters.oversized.Load(), "active_executions": active, "waiters": waiters, "waiter_limit": s.cfg.MaxConcurrent * 4}
}

type cacheEntry struct {
	Schema      int             `json:"schema"`
	Value       completionValue `json:"value"`
	UsageSource string          `json:"usage_source"`
}

func exactCacheKey(snapshot *routingSnapshot, key storage.Key, request Request, raw []byte) string {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var original any
	_ = decoder.Decode(&original)
	canonical, _ := json.Marshal(struct {
		Key, Alias, Revision, AdapterSchema string
		EffectiveMaxTokens                  int
		Original                            any
	}{key.ID, request.Model, snapshot.fingerprints[request.Model], "openai-anthropic-text/v1", *request.MaxTokens, original})
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}
func validCachedValue(data []byte, route Route) (completionValue, bool) {
	var entry cacheEntry
	if StrictJSON(data, &entry) != nil || entry.Schema != 1 || entry.Value.Message.Role != "assistant" {
		return completionValue{}, false
	}
	if _, err := finishReason("openai", entry.Value.Finish); err != nil {
		return completionValue{}, false
	}
	allowed := false
	for _, target := range route.Targets {
		if target.Provider == entry.Value.Provider {
			allowed = true
		}
	}
	if !allowed {
		return completionValue{}, false
	}
	if entry.Value.Usage == nil {
		if entry.UsageSource != "unknown" {
			return completionValue{}, false
		}
	} else if !entry.Value.Usage.valid() || entry.UsageSource != "provider_reported" {
		return completionValue{}, false
	}
	return entry.Value, true
}
func setInferenceHeaders(w http.ResponseWriter, provider, cacheState string, remaining int64) {
	w.Header().Set("X-Gateway-Provider", provider)
	w.Header().Set("X-Cache", cacheState)
	w.Header().Set("X-RateLimit-Remaining-Tokens", strconv.FormatInt(max(int64(0), remaining), 10))
}
func (s *Server) writeInferenceError(w http.ResponseWriter, result inferenceResult) {
	if result.status == 429 {
		w.Header().Set("X-RateLimit-Remaining-Tokens", strconv.FormatInt(max(int64(0), result.remaining), 10))
		if result.retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(int64((result.retryAfter+time.Second-1)/time.Second), 10))
		}
	}
	s.writeError(w, result.status, result.code)
}
func (s *Server) writeCompletion(w http.ResponseWriter, r *http.Request, id, alias string, value completionValue) error {
	if r.Context().Err() != nil {
		w.Header().Del("X-Gateway-Provider")
		w.Header().Del("X-Cache")
		status, code := upstreamError(r.Context())
		s.writeError(w, status, code)
		return r.Context().Err()
	}
	completion := Completion{ID: id, Object: "chat.completion", Created: time.Now().Unix(), Model: alias, Choices: []Choice{{Index: 0, Message: &value.Message, FinishReason: &value.Finish}}, Usage: value.Usage}
	body, err := json.Marshal(completion)
	if err != nil {
		return err
	}
	if int64(len(body)+1) > s.cfg.MaxOutputBytes {
		w.Header().Del("X-Gateway-Provider")
		w.Header().Del("X-Cache")
		s.writeError(w, 502, "upstream_failure")
		return errProtocol
	}
	_ = http.NewResponseController(w).SetWriteDeadline(writeDeadline(r.Context(), time.Duration(s.cfg.WriteTimeout)))
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(append(body, '\n'))
	return err
}
func reservationFailure(ctx context.Context, err error) inferenceResult {
	if ctx.Err() != nil {
		return failureResult(ctx)
	}
	var denied *limits.Denied
	if errors.As(err, &denied) {
		return inferenceResult{status: 429, code: "rate_limit_exceeded", outcome: "rate_limit_exceeded", remaining: denied.Remaining, retryAfter: denied.RetryAfter}
	}
	return unavailableResult()
}
func (s *Server) serveReusable(w http.ResponseWriter, r *http.Request, id string, key storage.Key, snapshot *routingSnapshot, request Request, raw []byte, record *storage.Record) string {
	ctx := r.Context()
	if err := acquire(ctx, s.slots); err != nil {
		result := failureResult(ctx)
		s.writeInferenceError(w, result)
		return result.outcome
	}
	slotHeld := true
	defer func() {
		if slotHeld {
			<-s.slots
		}
	}()
	route := snapshot.config.Routes[request.Model]
	target := route.Targets[0]
	provider := snapshot.config.Providers[target.Provider]
	reservation, err := s.limiter.Reserve(ctx, key.ID, id+"-admission", estimateInput(request, provider.Kind)+int64(*request.MaxTokens), limits.Policy{Capacity: key.Capacity, RefillPerSecond: key.RefillPerSecond})
	if err != nil {
		result := reservationFailure(ctx, err)
		s.writeInferenceError(w, result)
		return result.outcome
	}
	cacheKey := exactCacheKey(snapshot, key, request, raw)
	data, hit, cacheErr := s.cache.Get(ctx, cacheKey, route.Cache.MaxEntryBytes)
	var value completionValue
	if hit {
		value, hit = validCachedValue(data, route)
		if !hit {
			cacheErr = errProtocol
		}
	}
	if cacheErr != nil {
		hit = false
		s.reuseCounters.readErrors.Add(1)
	}
	if hit {
		if s.release(reservation) != nil {
			result := unavailableResult()
			s.writeInferenceError(w, result)
			return result.outcome
		}
		if ctx.Err() != nil {
			result := failureResult(ctx)
			s.writeInferenceError(w, result)
			return result.outcome
		}
		record.CacheState = "HIT"
		s.reuseCounters.hits.Add(1)
	} else {
		s.reuseCounters.misses.Add(1)
		if ctx.Err() != nil {
			s.release(reservation)
			result := failureResult(ctx)
			s.writeInferenceError(w, result)
			return result.outcome
		}
		deadline, _ := ctx.Deadline()
		configRevision := record.ConfigRevision
		call, leader, joinErr := s.flights.join(cacheKey, deadline, func(shared context.Context, executionID string) inferenceResult {
			defer func() { <-s.slots }()
			started := time.Now()
			result := s.runRoute(shared, snapshot, key, request, executionID, &reservation, nil)
			if result.status == 0 && result.outcome == "success" {
				source := "unknown"
				if result.value.Usage != nil {
					source = "provider_reported"
				}
				entry, _ := json.Marshal(cacheEntry{Schema: 1, Value: result.value, UsageSource: source})
				if len(entry) <= route.Cache.MaxEntryBytes {
					if s.cache.Put(shared, cacheKey, entry, time.Duration(route.Cache.TTL)) != nil {
						s.reuseCounters.writeErrors.Add(1)
					}
				} else {
					s.reuseCounters.oversized.Add(1)
				}
			}
			s.usage.Enqueue(storage.Record{ID: executionID, KeyID: key.ID, Alias: request.Model, Provider: result.value.Provider, Outcome: result.outcome, StartedMS: started.UnixMilli(), DurationMS: time.Since(started).Milliseconds(), Kind: "execution", CacheState: "MISS", ConfigRevision: configRevision, Attempts: result.attempts})
			return result
		})
		if joinErr != nil {
			s.release(reservation)
			result := unavailableResult()
			s.writeInferenceError(w, result)
			return result.outcome
		}
		defer s.flights.leave()
		record.ExecutionID = call.id
		if leader {
			slotHeld = false
			record.CacheState = "MISS"
		} else {
			record.CacheState = "COALESCED"
			s.reuseCounters.coalesced.Add(1)
			if s.release(reservation) != nil {
				result := unavailableResult()
				s.writeInferenceError(w, result)
				return result.outcome
			}
			<-s.slots
			slotHeld = false
		}
		select {
		case <-ctx.Done():
			result := failureResult(ctx)
			s.writeInferenceError(w, result)
			return result.outcome
		case <-call.done:
		}
		result := call.result
		if result.status != 0 {
			s.writeInferenceError(w, result)
			record.Provider = result.value.Provider
			return result.outcome
		}
		value = result.value
		if leader {
			reservation.Remaining = result.remaining
		}
		// Bound output encoding/writing as well as execution. Waiters retain only
		// a shared result while awaiting this slot, not a per-caller output buffer.
		if err := acquire(ctx, s.slots); err != nil {
			failure := failureResult(ctx)
			s.writeInferenceError(w, failure)
			return failure.outcome
		}
		slotHeld = true
	}
	record.Provider = value.Provider
	if value.Usage != nil {
		record.ClientInput = &value.Usage.PromptTokens
		record.ClientOutput = &value.Usage.CompletionTokens
	}
	setInferenceHeaders(w, value.Provider, record.CacheState, reservation.Remaining)
	return completionOutcome(ctx, s.writeCompletion(w, r, id, request.Model, value))
}

func completionOutcome(ctx context.Context, err error) string {
	if err == nil {
		return "success"
	}
	if ctx.Err() != nil {
		return failureResult(ctx).outcome
	}
	if errors.Is(err, errProtocol) {
		return "upstream_failure"
	}
	return "downstream_failure"
}
