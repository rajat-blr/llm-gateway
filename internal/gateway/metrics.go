package gateway

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/rajat-blr/llm-gateway/internal/storage"
)

type metrics struct {
	registry                              *prometheus.Registry
	aliases, providers                    map[string]bool
	requests, attempts, tokens            *prometheus.CounterVec
	unknownUsage, fallbacks, breakerSkips *prometheus.CounterVec
	latency, attemptLatency, ttft         *prometheus.HistogramVec
	activeAttempts                        prometheus.Gauge
}

func (m *metrics) alias(s string) string {
	if m.aliases[s] {
		return s
	}
	return "other"
}
func (m *metrics) provider(s string) string {
	if m.providers[s] {
		return s
	}
	return "other"
}
func newMetrics(s *Server) *metrics {
	m := &metrics{registry: prometheus.NewRegistry(), aliases: map[string]bool{}, providers: map[string]bool{}}
	for alias := range s.cfg.Routes {
		m.aliases[alias] = true
	}
	for provider := range s.cfg.Providers {
		m.providers[provider] = true
	}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_" + name, Help: help}, labels)
		m.registry.MustRegister(c)
		return c
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_" + name, Help: help, Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}}, labels)
		m.registry.MustRegister(h)
		return h
	}
	m.requests = counter("requests_total", "Completed inference HTTP requests, including authentication failures.", "alias", "mode", "outcome", "cache")
	m.attempts = counter("attempts_total", "Actual upstream attempts, counted once per execution.", "alias", "provider", "outcome")
	m.tokens = counter("provider_tokens_total", "Provider-reported tokens; excludes cache deliveries.", "alias", "provider", "kind")
	m.unknownUsage = counter("unknown_usage_total", "Attempts without trustworthy provider usage.", "alias", "provider")
	m.fallbacks = counter("fallbacks_total", "Actual attempts using a secondary target.", "alias")
	m.breakerSkips = counter("breaker_skips_total", "Targets skipped by an open or occupied half-open breaker.", "provider")
	m.latency = histogram("request_duration_seconds", "Inference HTTP duration including admission and downstream delivery.", "alias", "mode")
	m.attemptLatency = histogram("attempt_duration_seconds", "Upstream attempt duration including streaming and settlement.", "alias", "provider")
	m.ttft = histogram("first_text_seconds", "Time from HTTP admission to first successfully flushed nonempty text delta.", "alias", "provider")
	m.activeAttempts = prometheus.NewGauge(prometheus.GaugeOpts{Name: "llmgw_active_attempts", Help: "Upstream attempts currently executing, including streaming and settlement."})
	m.registry.MustRegister(m.activeAttempts, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	gauge := func(name, help string, f func() float64) {
		m.registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "llmgw_" + name, Help: help}, f))
	}
	count := func(name, help string, f func() float64) {
		m.registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "llmgw_" + name, Help: help}, f))
	}
	gauge("active_work_slots", "Occupied inference work slots.", func() float64 { return float64(len(s.slots)) })
	gauge("active_streams", "Admitted streaming handlers.", func() float64 { return float64(len(s.streams)) })
	gauge("shared_executions", "Active shared executions.", func() float64 { n, _ := s.flights.stats(); return float64(n) })
	gauge("shared_waiters", "Callers waiting for shared executions.", func() float64 { _, n := s.flights.stats(); return float64(n) })
	gauge("usage_in_flight", "A usage persistence operation is executing.", func() float64 {
		if s.usage.Stats().InFlight {
			return 1
		}
		return 0
	})
	count("usage_written_total", "Persisted usage records.", func() float64 { return float64(s.usage.Stats().Written) })
	count("usage_dropped_attempts_total", "Attempt records lost with dropped usage records.", func() float64 { return float64(s.usage.Stats().DroppedAttempts) })
	gauge("usage_queue_depth", "Queued usage records.", func() float64 { return float64(s.usage.Stats().QueueDepth) })
	gauge("config_generation", "Currently published routing generation.", func() float64 { return float64(s.routing.Load().generation) })
	count("usage_dropped_total", "Usage records lost, including failed writes.", func() float64 { return float64(s.usage.Stats().Dropped) })
	count("usage_write_failures_total", "Failed usage persistence operations.", func() float64 { return float64(s.usage.Stats().WriteFailures) })
	count("accounting_errors_total", "Failed Redis settlements.", func() float64 { return float64(s.accountingErrors.Load()) })
	count("reload_accepted_total", "Accepted routing reloads.", func() float64 { return float64(s.reloadAccepted.Load()) })
	count("reload_rejected_total", "Rejected routing reloads.", func() float64 { return float64(s.reloadRejected.Load()) })
	for name, value := range map[string]func() uint64{"cache_hits": s.reuseCounters.hits.Load, "cache_misses": s.reuseCounters.misses.Load, "cache_coalesced": s.reuseCounters.coalesced.Load, "cache_read_errors": s.reuseCounters.readErrors.Load, "cache_write_errors": s.reuseCounters.writeErrors.Load, "cache_oversized": s.reuseCounters.oversized.Load} {
		count(name+"_total", "Exact cache events.", func() float64 { return float64(value()) })
	}
	// One series per fixed state: scraping never changes the breaker state.
	for _, state := range []string{"closed", "open", "half_open"} {
		gauge("breakers_"+state, "Current unique upstream breakers in this state.", func() float64 {
			var n float64
			for _, b := range s.routing.Load().breakers {
				b.mu.Lock()
				if b.state == state {
					n++
				}
				b.mu.Unlock()
			}
			return n
		})
	}
	return m
}

type observationKey struct{}
type requestObservation struct {
	start                       time.Time
	alias, mode, outcome, cache string
	firstText                   bool
}
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (m *metrics) observeRequest(o *requestObservation, w *statusWriter) {
	outcome := o.outcome
	if outcome == "" {
		switch w.status {
		case 401:
			outcome = "invalid_api_key"
		case 503:
			outcome = "dependency_unavailable"
		case 504:
			outcome = "deadline_exceeded"
		default:
			outcome = "invalid_request"
		}
	}
	cache := o.cache
	if cache == "" {
		cache = "BYPASS"
	}
	m.requests.WithLabelValues(m.alias(o.alias), o.mode, outcome, cache).Inc()
	m.latency.WithLabelValues(m.alias(o.alias), o.mode).Observe(time.Since(o.start).Seconds())
}
func (s *Server) observeFirstText(r *http.Request, alias, provider string) {
	if o, ok := r.Context().Value(observationKey{}).(*requestObservation); ok && !o.firstText {
		o.firstText = true
		s.metrics.ttft.WithLabelValues(s.metrics.alias(alias), s.metrics.provider(provider)).Observe(time.Since(o.start).Seconds())
	}
}
func (m *metrics) observeAttempts(alias string, attempts []storage.Attempt) {
	for _, a := range attempts {
		labels := []string{m.alias(alias), m.provider(a.Provider)}
		m.attempts.WithLabelValues(labels[0], labels[1], a.Outcome).Inc()
		m.attemptLatency.WithLabelValues(labels...).Observe(float64(a.DurationMS) / 1000)
		if a.InputTokens == nil || a.OutputTokens == nil {
			m.unknownUsage.WithLabelValues(labels...).Inc()
		} else {
			m.tokens.WithLabelValues(labels[0], labels[1], "input").Add(float64(*a.InputTokens))
			m.tokens.WithLabelValues(labels[0], labels[1], "output").Add(float64(*a.OutputTokens))
		}
	}
}
