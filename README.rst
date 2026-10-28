LLM Gateway in Go
=================

An OpenAI-compatible text gateway that puts authentication, routing, token
budgets, streaming, and usage visibility behind one API. Clients call a stable
model alias; the gateway handles provider selection and delivery.

Built in Go with Redis for shared token admission, SQLite for keys and usage,
and Prometheus/Grafana for observability. The browser demo uses live OpenAI;
a deterministic fixture mode runs without a paid API key. MIT licensed.

What the app does
-----------------

* **One client-facing API.** Accepts text chat requests through
  ``POST /v1/chat/completions`` and exposes authorized aliases through
  ``GET /v1/models``. Normalizes JSON responses and server-sent events into
  OpenAI-compatible shapes, including usage and sanitized errors.
* **Controlled access.** Bearer keys carry allowed aliases, output limits,
  token-bucket policies, and explicit reuse permission. Keys are stored as
  hashes in SQLite; revocation takes effect on the next request.
* **Bounded streaming.** Relays text as it arrives, propagates cancellation,
  and enforces concurrency, event-size, response-size, idle, and overall
  deadlines. Slow readers apply backpressure to upstream reads.
* **Resilient routing.** Routes an alias to an ordered primary/backup pair,
  with at most two attempts. Per-upstream circuit breakers isolate repeated
  transient failures. Fallback is allowed only before the response is committed.
* **Shared token admission.** Redis reservations enforce per-key token
  budgets across gateway instances. Reliable final usage reconciles the debit;
  interrupted or unknown usage retains a conservative reservation.
* **Optional exact reuse.** Eligible non-streaming requests can use a bounded
  Redis response cache or join one in-process execution. Both route settings
  and key policy must permit reuse. Streaming always bypasses it.
* **Routing reloads.** SIGHUP atomically publishes validated route changes.
  In-flight requests retain their original routing snapshot; invalid changes
  leave the last good configuration active.
* **Visible operations.** Records client outcomes separately from provider
  attempts, exposes protected usage/admin APIs, and includes a provisioned
  Grafana dashboard. Accounting loss is reported explicitly.

Interactive demo
----------------

The browser UI pairs an editable request with its response and diagnostics:
HTTP status, serving provider, cache state, first-text latency, total tokens,
elapsed time, and request ID. Expandable request and response JSON make the
API behavior inspectable. The provider credential stays server-side.

Eight examples cover the supported flow and its boundaries:

* OpenAI streaming with final token usage.
* Stop-sequence handling.
* A JSON completion with temperature and output controls.
* System instructions.
* Multi-turn conversation supplied in one request.
* Authenticated model-alias discovery.
* Unsupported tools, rejected with HTTP 400.
* An unavailable alias, rejected with HTTP 403.

Live mode answers the actual prompts. Fixture mode returns a fixed greeting
for deterministic checks, including system and conversation examples.
The gateway does not store conversation history; clients send it with each call.

How requests move through the gateway
-------------------------------------

::

    Client / browser / OpenAI SDK
                |
                v
    SQLite key lookup -> alias and request validation
                |
                v
    Bounded admission -> Redis token reservation
                |
                v
    Route snapshot -> circuit breaker -> provider attempt
                |                         |
                |                  primary / backup
                v
    Normalize JSON or stream SSE -> client
                |
                v
    Reconcile tokens + queue usage metadata
                |
                +-> SQLite usage / admin API
                +-> Prometheus -> Grafana

Eligible non-streaming calls also pass through exact-cache lookup and local
request coalescing. Completed cache entries are shared through Redis;
in-flight coalescing is local to each gateway process.

The implementation separates HTTP delivery and routing in ``internal/gateway``,
Redis admission in ``internal/limits``, response reuse in ``internal/reuse``,
and SQLite persistence in ``internal/storage``. The browser assets are embedded
in the gateway binary; Compose supplies Redis and optional monitoring.

Correctness under failure
-------------------------

**Streaming ownership.** The handler owns and closes the upstream response
body, reading synchronously without a separate producer queue. First-event
parsing happens before committing headers. A later upstream failure produces
a sanitized SSE error and closes without a successful ``[DONE]`` marker;
the gateway never switches providers midway through an answer.

**Accounting ownership.** Each provider attempt has its own reservation and
usage record. Client deliveries, cached responses, and coalesced callers do
not multiply recorded provider consumption. Unknown usage remains unknown
instead of being reported as zero.

**Dependency behavior.** Redis admission failure rejects new inference;
already active streams can continue. SQLite key-read failure rejects access.
Usage-only write failures can leave inference available, with queue overflow,
dropped records, and write failures exposed through counters.

**Shutdown and reload.** Shutdown stops admission and drains HTTP work, shared
executions, and queued usage within a finite budget. Routing reloads require
monotonic revisions for changed behavior and preserve breaker state for
unchanged upstream identities.

Observability and administration
--------------------------------

The Grafana dashboard covers request rate, client and first-text latency,
active work, upstream attempts, provider tokens, cache/coalescing, rate limits,
circuit breakers, usage loss, CPU/goroutines, memory, and routing reloads.
Metrics run on a separate management listener with bounded labels that exclude
keys, prompts, concrete model names, and request IDs. Optional local pprof
supports CPU/heap investigation.

The separately authenticated admin API supports key creation/listing/revocation,
hourly usage aggregates, and dependency/accounting status. SQLite usage records
contain metadata, not prompts, completions, credentials, or upstream error bodies.
Opt-in response caching does store completion text in Redis, subject to TTL
and entry-size bounds.

Scope and verified results
--------------------------

This is a text-chat gateway with an intentionally explicit compatibility
contract. Supported controls include ``temperature``, ``top_p``, ``max_tokens``,
``stop``, ``n=1``, and requested stream usage. Tools, non-text messages, unknown
fields, and unsupported roles are rejected before upstream work. See the
`compatibility matrix <docs/compatibility.rst>`_ for exact constraints.

The browser demo exposes ``default-chat`` through OpenAI. An Anthropic adapter
is covered by isolated regression fixtures and is not offered in the demo.
Recorded OpenAI JSON/SSE live checks passed on October 28, 2026; Anthropic live
behavior remains unverified. The configured live route is in
``configs/openai.json``; provider/model lifecycle changes require configuration
maintenance.

Checked-in local evidence includes:

* **Compatibility and failure handling:** race-tested Go suites, 20 pinned
  Python SDK scenarios, differential JSON fuzzing, browser-client contract
  checks, Redis interruption, and monitored Docker acceptance.
* **Load:** 468,148 requests across 93 accepted non-fault measured stages.
  Cache hits delivered 83,074 completions without provider attempts; three
  100-caller coalescing batches each owned one provider attempt.
* **Resources:** three 120-second runs completed 15,696 streams and canceled
  another 15,696. Cancellation was observed within 226 ms; quiescent goroutines
  stayed at 63–64 and TCP sockets at 48–64 in those runs.
* **Profiling:** the measured 32 KiB strict-JSON parser case improved from
  median 85.3 to 73.3 microseconds and approximately 329,607 to 164,770 allocated
  bytes per operation. The full-gateway comparison did not show a throughput gain.

These are finite, fixture-backed local measurements, not production capacity
claims. Concurrency-500 warmup produced 57 transient HTTP 502 failures; a deliberate
SQLite writer lock lost 27 usage records while 2,450 inference requests completed.
The bounded in-memory usage queue is not crash-safe, and token reservations are
not an exact spending cap or billing guarantee. Raw evidence retains these limits.

Try it locally
--------------

Requires Docker with Compose. Start the deterministic demo without a paid key::

    make demo

Open http://localhost:8080 and select **Run stream**. To include Grafana and
Prometheus, use ``make monitoring`` instead; the dashboard is at
http://localhost:3000/d/llm-gateway.

For live OpenAI, set ``OPENAI_API_KEY`` in the ignored ``.env`` file and run
``make demo-openai``. Live requests incur provider usage; monitoring is included.
The included gateway/admin tokens are public local demo credentials. Stop the
stack with ``make down``; named volumes are preserved.

Documentation
-------------

* `Operations reference <docs/operations.rst>`_: setup, API examples,
  configuration, key management, reuse/reload behavior, and validation commands.
* `Architecture and design decisions <docs/architecture.txt>`_: streaming,
  accounting, concurrency, persistence, and observability boundaries.
* `Compatibility matrix <docs/compatibility.rst>`_: supported request/response
  contract and explicit rejections.
* `Release acceptance <docs/release-acceptance.rst>`_: executable checks and
  evidence mapped to each capability.
* `Benchmark guide <benchmarks/README.rst>`_ and
  `extended load method <benchmarks/extended-method.rst>`_: measurements and limits.
* `Resource soak <benchmarks/resource-soak.rst>`_ and
  `profiling investigation <benchmarks/profiles/investigation.rst>`_: sustained
  behavior and parser optimization evidence.
