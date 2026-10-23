# PRD review and delivery plan

Source: `LLM Gateway in Go - Revised PRD.pdf` (October 23, 2026).

The scope is clear and appropriately focused on demonstrable concurrency,
resilience, and evidence. This is a portfolio project, not a billing system.
The release requires a gateway binary, Redis, and SQLite; a mock must make the
demo independent of paid API access. Nothing in this initial implementation
should be interpreted as completion of the entire v1 release.

## Milestones and commit boundaries

1. Project baseline: MIT license, PRD review, compatibility decisions, Go module.
2. Protocol foundation: strict request contract, environment-backed bootstrap
   key, OpenAI and Anthropic HTTP adapters, bounded pull-based SSE, mock,
   deadlines, graceful shutdown, and executable contract tests.
3. Reviewer setup: container demo, CI race checks, SDK integration example,
   architecture and stream ownership decision. Record validation limitations.
4. Resilience: priority routing with at most two attempts, fake-clock breakers,
   per-attempt Redis Lua reservations, idempotent reconciliation, outage policy.
5. Evidence: SQLite keys and bounded usage worker, admin API, exact caching,
   singleflight, immutable SIGHUP reload, Prometheus and Grafana.
6. Performance: seeded failure scenarios, comparable direct/gateway load runs,
   repeated measurements, profiles, and one measured optimization.

## Decisions that remove ambiguity

- Use a standard-library-only protocol foundation. Add Redis and SQLite
  dependencies when those milestones can be tested together.
- Bootstrap authentication uses a mandatory environment key with SHA-256
  comparison. This is a temporary milestone mechanism; v1 must query SQLite
  on authentication so revocation is immediate.
- A route initially has exactly one target. Do not advertise fallback before
  per-attempt accounting exists. Unsupported fields fail before network I/O.
- Preserve omitted generation inputs with pointer fields. Default max_tokens
  to the route bound; expose that bound in configuration.
- Anthropic's text subset permits only leading system messages, followed by
  at least one user message; reject assistant prefilling. Reject simultaneous
  temperature and top_p for Anthropic. Validate these restrictions explicitly.
- A stream is read synchronously by its owner with no producer goroutine or
  queue. The response body is always closed. Limit SSE event bytes, total
  output bytes, response headers, concurrency, request lifetime, and writes.
- Until token accounting exists, omit X-RateLimit-Remaining-Tokens rather than
  invent an allowance. Cache always reports BYPASS.

## Release gates still outstanding

All acceptance cases in the PRD remain release gates, including two-instance
Redis tests, SQLite contention/loss signals, fake-clock breaker tests, SDK
mid-stream errors, cancellation/resource soak tests, reload snapshot pinning,
and measured performance. No benchmark claims are made before measurement.
