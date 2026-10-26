LLM Gateway in Go
=================

A small OpenAI-compatible text gateway with OpenAI and Anthropic adapters.
MIT licensed. The default demo uses deterministic fixtures; no paid API key
is needed.

Status: keys and usage foundation
---------------------------------

Implemented: strict text request validation, SQLite-backed bearer authentication,
alias discovery, translated JSON/SSE responses, bounded concurrency and
stream sizes, cancellation, deadlines, graceful shutdown, shared Redis token
reservations, priority fallback with at most two attempts, and local circuit
breakers, SQLite-backed keys with immediate revocation, protected key/usage
admin endpoints, and a bounded usage worker with visible loss counters.
Fixtures and pinned SDK acceptance tests require no paid API key.

The complete v1 release remains in progress. Caching/coalescing, SIGHUP
reload, management metrics, dashboards, soak tests, and measured benchmarks
are pending. Readiness checks SQLite key reads, Redis, and validated startup
state; provider failure does not affect readiness. No performance claims
are made.

Run the demo
------------

Requires Docker Desktop or Docker Engine with Compose::

    make demo

Open http://localhost:8080 in your browser and select Run stream. The page
includes the public local demo key and can exercise either adapter. It
does not save keys. If you set a custom GATEWAY_API_KEY, enter it in the form.

Or call the API directly::

    curl -N http://localhost:8080/v1/chat/completions \
      -H 'Authorization: Bearer demo-local-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"default-chat","messages":[{"role":"user","content":"Hello"}],"stream":true,"stream_options":{"include_usage":true}}'

Use ``anthropic-chat`` to exercise the Anthropic translation. Both aliases
return fixture text. The gateway port is published on loopback only. The
included demo key is public fixture data; set GATEWAY_API_KEY for your own
local key. Stop the demo with ``make down``.

Run locally with Go 1.26 or newer, starting Redis first::

    docker compose up -d --wait redis
    make build
    ./bin/mock

In a second terminal, start the backup fixture::

    ./bin/mock -listen :9091

In a third terminal, start the gateway::

    export GATEWAY_ADMIN_TOKEN=local-example-admin-token
    export GATEWAY_API_KEY=local-example-key
    export MOCK_API_KEY=fixture-only
    ./bin/gateway -config configs/demo.json

The mock supports ``-first-delay``, ``-chunk-delay``, ``-chunks``, ``-payload``,
``-fail-every``, and ``-drop-after``. For example, ``./bin/mock -drop-after 1``
produces a mid-stream failure, and ``./bin/mock -fail-every 1`` always returns
HTTP 503 before streaming. GET /stats exposes mock call and cancellation
counts locally; it is not published by Compose.

Validation
----------

Go unit, HTTP contract, real Redis, and two-gateway admission tests, with
race detection and vet::

    docker compose up -d --wait redis
    make check
    make test-fuzz

Pinned OpenAI Python SDK tests launch isolated gateway/mock subprocesses and
exercise both adapters, unsupported input, pre-commit failure, mid-stream
errors, disconnect cancellation, request deadlines, and bounded shutdown::

    docker compose up -d --wait redis
    python3 -m venv /tmp/gateway-sdk
    /tmp/gateway-sdk/bin/pip install -r examples/requirements.txt
    make build
    /tmp/gateway-sdk/bin/python scripts/sdk_integration.py

To verify a real Redis interruption while streaming (the service is restored
automatically)::

    /tmp/gateway-sdk/bin/python scripts/redis_outage_acceptance.py

To use the SDK with the running Docker demo::

    /tmp/gateway-sdk/bin/python examples/chat.py

The default test Redis address is 127.0.0.1:6381. Set REDIS_TEST_ADDR to
use a separate test instance. Test namespaces are isolated from demo data.
Running go test directly without REDIS_TEST_ADDR skips real-Redis scenarios;
make check and CI require them.

CI runs the Go checks, 15 SDK scenarios, the admin demo, container failure
scenarios, and a real Redis interruption while streaming. Real-provider
smoke tests are intentionally outside deterministic tests.

API subset
----------

POST /v1/chat/completions accepts model, messages, stream, temperature,
top_p, max_tokens, stop, n=1, and stream_options.include_usage. Messages
contain nonempty text with system/user/assistant roles. Unknown members,
duplicate members, nulls, nontext messages, unsupported roles, and trailing
JSON are rejected with HTTP 400 before upstream I/O. Temperature is 0..1,
top_p is greater than 0 and at most 1, and stop has 1..4 nonempty strings.
An omitted max_tokens uses the route maximum; an explicit value must fit it.
Anthropic additionally requires leading system messages, a first and last
user message, and at most one of temperature/top_p.

GET /v1/models requires the bearer key. GET /healthz and GET /readyz are
unauthenticated. Model discovery and inference enforce the calling key's alias list. The optional
bootstrap import initially allows all aliases, subject to its stored policy.
Errors contain error.message, error.type, and error.code. Current status
mappings: 400 invalid_request; 401 invalid_api_key; 403 model_not_allowed;
429 rate_limit_exceeded; 502 upstream_failure; 503 dependency_unavailable
for Redis/draining; 503 all_providers_unavailable when all breakers reject;
504 deadline_exceeded. Upstream error bodies and credentials are withheld.

Successful calls expose X-Request-ID, X-Gateway-Provider, and X-Cache=BYPASS.
X-RateLimit-Remaining-Tokens reports available tokens at the serving
attempt's reservation, before final reconciliation; it is an admission
snapshot, not a final balance. A 429 can include Retry-After. Responses
use a gateway ID and the requested alias, including all stream chunks.
Usage is sent in a separate empty-choices chunk only when requested and
known. A failed stream emits a sanitized error data event and closes
without [DONE]. The pinned SDK raises APIError for that event.

Configuration and bounds
------------------------

The JSON config is validated before the listener starts. Routes contain an
ordered targets array with one or two entries, each specifying provider,
model, and attempt_timeout; the old single provider/model route format is
rejected. Each provider has
kind (openai/anthropic), a base_url including /v1, and a credential key_env.
The gateway key also comes from its named environment variable. For real
providers, change the base URL, upstream model, and environment variable;
never commit credentials. Redirects are disabled to prevent forwarding
credentials to a redirect destination. Configure text-capable models that
support the documented subset; reasoning models are outside this contract.

Demo defaults: 1 MiB request body, 8 MiB upstream response/stream budget,
64 KiB SSE event, 64 concurrent upstream calls, 32 active streams,
30-second overall timeout, 5-second upstream idle/header/TLS timeout,
5-second downstream write timeout, 10-second shutdown budget, 25-second per-attempt timeout, and at most
1,024 requested output tokens. Admission waiting consumes the overall
deadline. Slow clients are bounded by write timeouts. Error delivery has a
separate bounded best-effort write window after a timeout.

Design and remaining release gates are recorded in docs/architecture.txt.
The local PRD and all Markdown files are excluded from tracking by request.

Resilience demo
---------------

Fail the primary before streaming; open the browser demo and Run stream.
The serving provider should become backup-anthropic::

    MOCK_FAIL_EVERY=1 docker compose up --build --force-recreate --wait

Drop the primary after one text chunk. The page should show a partial response
and an explicit error; the backup must not execute for that request::

    MOCK_DROP_AFTER=1 docker compose up --build --force-recreate --wait

Recreation resets local breakers between scenarios. After five transient
failures in 30 seconds a breaker opens for 10 seconds. One half-open probe is
allowed at a time; one successful probe closes it. Provider credentials and
upstream models scope breaker state. Invalid input, provider authentication
errors, client cancellation, and Redis errors do not count as failures.
Restore the normal demo with::

    docker compose up --build --force-recreate --wait

Retry backoff has jitter, a 25 ms base and a 250 ms cap. Retry-After is honored
only if it fits the remaining overall deadline; otherwise fallback stops.
Only transport/timeouts, HTTP 429, and HTTP 5xx permit fallback before response
commitment. Malformed protocol data and other HTTP 4xx do not. After headers
or data are committed, failures close the stream without [DONE].

Shared token accounting
-----------------------

The bootstrap key has a shared 100,000-token capacity and a 1,000-token/second
refill. Each attempt reserves estimated input plus max_tokens. OpenAI input
estimation is 2 framing tokens plus 4 per message plus ceil(UTF-8 text bytes/4);
Anthropic uses 4 framing tokens and 6 per message with the same text estimate.
This approximate admission rule is not an exact spending cap. Actual usage
can exceed the estimate and produce a negative balance that must refill.

Redis Lua uses Redis server time and integer milli-tokens. Reconciliation is
idempotent by reservation ID. Complete, reliable provider usage adjusts the
debit to input plus output tokens. Missing/partial usage, timeouts, and broken
streams retain the full debit. Before a second attempt, the first must be
settled and a new reservation must succeed. Reconciliation errors stop new
attempts and emit an accounting-error log/counter; an already streaming call
continues. Accounting errors are visible through /admin/status; the
Prometheus management exporter is pending.

Reservation metadata expires after 15 minutes without automatic refunds.
Bucket TTL covers debt refill plus the reconciliation lifetime. Different
bucket policies for the same key identity fail closed. Redis outage rejects
new inference with 503; active streams continue. There is no local limiter
fallback. Stable SQLite key IDs identify Redis buckets. Each key's stored capacity and
refill are pinned in its reservation and used during reconciliation. Legacy
experimental buckets identified by bootstrap hashes are not migrated.

Compose uses a 128 MiB Redis noeviction policy with AOF on a named volume.
No eviction is allowed to silently restore a bucket. Persistence settings and
Redis data loss affect accounting durability; this is not billing-grade.
Client outcomes and every upstream attempt are also persisted in SQLite
through a bounded worker, subject to the loss behavior below.

Keys and admin API
------------------

GATEWAY_ADMIN_TOKEN is mandatory and must differ from the optional bootstrap
GATEWAY_API_KEY. Admin authorization is separate from inference authorization.
Compose includes public local fixture tokens; custom deployments should set
their own values. The admin token is never accepted for inference.

Run the temporary-key reviewer demo (it prints no credential and revokes only
the key it creates)::

    /tmp/gateway-sdk/bin/python scripts/admin_demo.py

Or inspect the running local demo::

    curl http://localhost:8080/admin/keys \
      -H 'Authorization: Bearer demo-local-admin-token'
    curl http://localhost:8080/admin/usage \
      -H 'Authorization: Bearer demo-local-admin-token'
    curl http://localhost:8080/admin/status \
      -H 'Authorization: Bearer demo-local-admin-token'

POST /admin/keys accepts name, aliases, capacity, refill_per_second,
max_output_tokens, and allow_reuse. Example JSON::

    {"name":"reviewer","aliases":["default-chat"],"capacity":1000,
     "refill_per_second":10,"max_output_tokens":64,"allow_reuse":false}

The 201 response includes the one-time key and its metadata. Only SHA-256
hashes are stored; prefix is a hash fingerprint, not a fragment of the secret.
List responses never expose secrets or full credential hashes. GET /admin/keys supports
limit=1..100 and offset=0..10000. DELETE /admin/keys/{id} revokes immediately
for the next request and returns 204, including on repeat revocation. Existing
admitted requests continue under their pinned identity. Unknown IDs give 404.

Every inference/model-list call queries SQLite; there is no key cache. Missing
or revoked keys return 401, forbidden aliases return 403, and unavailable key
storage returns 503. Effective max_tokens is bounded by both key and route.
Key policies are immutable in this milestone; create a new key and revoke the
old one to change policy. allow_reuse stores permission for future opt-in
caching; all routes currently bypass caching.

GATEWAY_API_KEY is an optional first-use SQLite import, never an authentication
bypass. Restart does not resurrect a revoked bootstrap key or overwrite its
stored policy/aliases. Changing the environment value imports another key; it
does not revoke previous keys. With no bootstrap environment value, use the
admin API to create the first key.

Usage persistence and limits
----------------------------

The CGO-free SQLite driver keeps the gateway a single binary. Compose stores
the database in the gateway-data volume; local Go runs use data/gateway.db.
WAL mode allows reads alongside a writer. One pooled writer serializes key
changes and usage writes; four read connections serve authentication/admin
queries. Defaults: 100 ms busy timeout, 500 ms operation timeout, and a
1,024-record queue plus at most one record being written. SQL busy/disk errors
are bounded and surfaced. Place the database on a local filesystem and keep
its WAL/SHM files with it; do not move or replace an open database.

Each queued record contains one client outcome and at most two upstream
attempts, committed in one transaction. Prompts, completions, credentials, and
upstream error bodies are absent from the schema. Client completion usage is
separate from provider attempt usage. Interrupted or partial usage stays
unknown; a failed primary still has an attempt record when backup succeeds.

GET /admin/usage aggregates hourly UTC buckets by time, key, alias, provider,
and outcome. Optional filters: from/to (RFC3339, a half-open range of at most
31 days), key_id, alias, provider, and limit=1..1000 per result section. The
default range is the previous 24 hours. Requests group by serving provider;
attempts group by attempted provider. Unknown usage is counted explicitly,
rather than treated as measured zero consumption. Truncated reports set
truncated=true; narrow the time/key filters to inspect additional groups.

Queue overflow keeps serving and increments dropped_records/dropped_attempts.
Write failure also increments write_failures. GET /admin/status exposes queue
capacity/depth, in-flight state, process-lifetime enqueue/write/drop counters,
SQLite/Redis availability, and Redis accounting errors even when SQLite is
unavailable. /admin/usage includes the same persistence counters when its query
succeeds. Readiness checks SQLite key reads; a usage-only write failure can
keep inference available while its loss counters increase.

Shutdown stops admission and drains HTTP work and queued usage within one
shared finite deadline. Deadline exhaustion cancels writes, logs undrained
work, and exits with failure. The queue is memory-only: overflow, write error,
forced shutdown, and process crash can lose records. Counters reset on restart;
persisted usage remains. This is explicitly not billing-grade durability.
