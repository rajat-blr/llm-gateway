LLM Gateway in Go
=================

A small OpenAI-compatible text gateway with OpenAI and Anthropic adapters.
MIT licensed. The default demo uses deterministic fixtures; no paid API key
is needed.

Status: protocol foundation
---------------------------

Implemented: strict text request validation, bootstrap bearer authentication,
alias discovery, JSON and translated SSE responses, usage chunks, bounded
upstream concurrency and stream sizes, cancellation, timeouts, graceful
shutdown, deterministic failure fixtures, and pinned SDK acceptance tests.

The complete v1 release remains in progress. Redis reservations, fallback,
circuit breakers, SQLite-backed keys and usage, caching/coalescing, SIGHUP
reload, metrics, dashboards, and measured benchmark results are not yet
implemented. This milestone uses one target per alias and an environment
bootstrap key. Readiness checks the validated startup state; it will need
store checks when required stores are introduced. Provider failure does not
affect readiness. No performance claims are made.

Run the demo
------------

Requires Docker Desktop or Docker Engine with Compose::

    make demo
    curl -N http://localhost:8080/v1/chat/completions \
      -H 'Authorization: Bearer demo-local-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"default-chat","messages":[{"role":"user","content":"Hello"}],"stream":true,"stream_options":{"include_usage":true}}'

Use ``anthropic-chat`` to exercise the Anthropic translation. Both aliases
return fixture text. The gateway port is published on loopback only. The
included demo key is public fixture data; set GATEWAY_API_KEY for your own
local key. Stop the demo with ``make down``.

Run locally with Go 1.23 or newer::

    make build
    ./bin/mock

In another terminal::

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

Go unit and HTTP contract tests, race detection, and vet::

    make check
    make test-fuzz

Pinned OpenAI Python SDK tests launch isolated gateway/mock subprocesses and
exercise both adapters, unsupported input, pre-commit failure, mid-stream
errors, disconnect cancellation, request deadlines, and bounded shutdown::

    python3 -m venv /tmp/gateway-sdk
    /tmp/gateway-sdk/bin/pip install -r examples/requirements.txt
    make build
    /tmp/gateway-sdk/bin/python scripts/sdk_integration.py

To use the SDK with the running Docker demo::

    /tmp/gateway-sdk/bin/python examples/chat.py

CI runs the Go checks, SDK scenarios, and the container demo. Real-provider
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
unauthenticated. All configured aliases are available to the bootstrap key.
Errors contain error.message, error.type, and error.code. Current status
mappings: 400 invalid_request; 401 invalid_api_key; 403 model_not_allowed;
502 upstream_failure; 503 dependency_unavailable during draining;
504 deadline_exceeded. Upstream error bodies and credentials are withheld.

Successful calls expose X-Request-ID, X-Gateway-Provider, and X-Cache=BYPASS.
Rate-limit headers are omitted until shared accounting exists. Responses
use a gateway ID and the requested alias, including all stream chunks.
Usage is sent in a separate empty-choices chunk only when requested and
known. A failed stream emits a sanitized error data event and closes
without [DONE]. The pinned SDK raises APIError for that event.

Configuration and bounds
------------------------

The JSON config is validated before the listener starts. Each provider has
kind (openai/anthropic), a base_url including /v1, and a credential key_env.
The gateway key also comes from its named environment variable. For real
providers, change the base URL, upstream model, and environment variable;
never commit credentials. Redirects are disabled to prevent forwarding
credentials to a redirect destination. Configure text-capable models that
support the documented subset; reasoning models are outside this contract.

Demo defaults: 1 MiB request body, 8 MiB upstream response/stream budget,
64 KiB SSE event, 64 concurrent upstream calls, 32 active streams,
30-second overall timeout, 5-second upstream idle/header/TLS timeout,
5-second downstream write timeout, 10-second shutdown budget, and at most
1,024 requested output tokens. Admission waiting consumes the overall
deadline. Slow clients are bounded by write timeouts. Error delivery has a
separate bounded best-effort write window after a timeout.

Design and remaining release gates are recorded in docs/architecture.txt.
The local PRD and all Markdown files are excluded from tracking by request.
