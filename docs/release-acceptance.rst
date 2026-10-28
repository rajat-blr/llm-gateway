Version 1 acceptance map
========================

This is a portfolio text gateway, with fixtures independent of paid providers.
The checked-in compatibility matrix is docs/compatibility.rst. Architecture
and the stream ownership, accounting, and coalescing decisions are in
docs/architecture.txt. Redis and SQLite remain required dependencies.

Acceptance evidence
---------------------

* Compatibility: gateway contract/server/stream tests plus the 20-case pinned
  Python SDK suite; both adapters, supported input and explicit rejections.
* Fallback: routing_test.go covers classified failures, two-attempt cap,
  per-attempt reservations, Retry-After, attempt timeouts and no post-commit retry.
* Streaming/backpressure: stream/server tests enforce event/output bounds and
  deadlines. resource_soak.py adds repeated unread 4MiB responses with tiny
  receive buffers and 200ms write deadlines; resources drain before clients close.
* Cancellation: SDK disconnect tests and sustained resource_soak.py bursts
  verify mock cancellation within one second under local conditions and record
  quiescent goroutines, TCP sockets, RSS and post-GC retained heap.
* Shared limits: limits/redis_test.go and gateway/routing_test.go use real Redis
  with two instances, server-time refill, idempotent settlement, unknown-debit
  retention, expiry and required-store outage behavior.
* Exact reuse: cache/flights tests cover per-key isolation, expiry, corruption,
  output size, revisions, canceled initiator/followers and bounded shared jobs.
  release_load.py reports hits, misses and coalesced batches separately.
* Reload/shutdown: reload tests pin snapshots through fallback; SDK tests send
  actual SIGHUP/SIGTERM, validate rejected candidates and finite drain budgets.
* SQLite: storage/admin/worker tests cover WAL read availability, bounded busy
  writes, revocation, migration, queue overflow and explicit dropped/write-failure
  signals. The load matrix measures a real SQLite writer lock during inference.
* Redis interruption: redis_outage_acceptance.py stops/restores the local demo
  Redis, verifies new inference rejection and existing stream continuation.
* Visibility: metrics tests verify separate listeners, bounded labels and
  execution/client ownership. monitoring_acceptance.py verifies scraping,
  provisioned Grafana datasource/dashboard and every panel's query.
* Reviewer demo: eight browser examples, Node client contract checks, actual
  Docker round trips, key/reuse scripts, and CI fixture failure scenarios.
* Performance: raw baseline and extended matrix; CPU/heap/allocation profiles
  and a measured strict-JSON improvement with an original-parser fuzz oracle.

Run the deterministic gate with Redis on REDIS_TEST_ADDR and Go, Python SDK
requirements, Node.js 24 installed::

    make release-check

Full developer-host evidence, outside timing-sensitive CI::

    make release-evidence

The default full evidence run includes concurrency through 500 when this host
can run it, all load repetitions and three 120-second sustained soak runs.
The reports retain errors and loss counters, including deliberate failures.
No closed-loop fixture result establishes an open-loop or production capacity
limit. A finite soak cannot prove leak freedom. See benchmarks/ for raw data
and explicit experiment conditions.

Optional paid-provider checks are outside deterministic CI and require the selected provider
credentials and compatible model names in the environment or ignored .env::

    python3 scripts/live_smoke.py --provider openai --live

This makes at most two OpenAI requests. Select --provider anthropic to test only
Anthropic, or --provider both to test both (at most four requests). Each uses
64 output tokens per request. Results contain adapter/mode/pass metadata and
sanitized failure classes. Credentials and generated text are not written to
the report. Missing credentials prevent the test; fixtures do not count as
live-provider validation. Live-test execution status is recorded in README.rst.
