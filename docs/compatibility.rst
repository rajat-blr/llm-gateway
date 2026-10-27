Text API compatibility
=======================

The gateway accepts POST /v1/chat/completions and authenticated GET /v1/models.
OpenAI and Anthropic fixtures cover this common subset. The pinned SDK version
is in examples/requirements.txt. This is a text-chat compatibility subset;
model-specific reasoning APIs and Responses API are excluded.

============================ ============================ ==============================
Input                        OpenAI mapping               Anthropic mapping
============================ ============================ ==============================
model                        configured upstream model    configured upstream model
messages                     text messages                leading system -> system blocks
stream                       stream                       stream
max_tokens                   max_tokens                   max_tokens (mandatory upstream)
temperature                  temperature                  temperature
top_p                        top_p                        top_p
stop                         stop                         stop_sequences
n                            only 1                       one message
stream_options.include_usage final separate usage chunk   translated start/delta usage
============================ ============================ ==============================

A request has 1..256 nonempty text messages with system/user/assistant roles.
Temperature is finite in 0..1, top_p is greater than 0 and at most 1, max_tokens
is an integer from 1 through the minimum route/key bound. Omission uses that
bound. Stop is one nonempty string or 1..4 nonempty strings. Anthropic requires
leading system messages, first and last conversation roles user, and at most
one of temperature/top_p. Input is checked against every fallback target.

Unknown fields, duplicate object keys (including escaped equivalents), nulls,
invalid types, depth beyond 32, multiple JSON documents, tools, images, audio,
structured output and unsupported combinations fail before provider I/O.
Streaming options are valid only with stream=true. Payloads and events have
configured byte limits. Unknown fields in upstream provider payloads may be
ignored while decoding the supported response subset.

Successful JSON/SSE responses expose stable gateway request IDs and the alias,
with normalized choices, finish reason and usage when reliable. Usage is a
separate empty-choices SSE chunk when requested. Only successful completion
emits [DONE]. A committed stream failure emits a sanitized error event where
possible and closes; it never invokes another provider.

==== ========================= =============================================
HTTP Code                      Meaning
==== ========================= =============================================
400  invalid_request           invalid or unsupported request
401  invalid_api_key           missing, invalid or revoked gateway key
403  model_not_allowed         alias unavailable to the calling key
429  rate_limit_exceeded       shared token admission denied
502  upstream_failure          transport, protocol or provider failure
503  all_providers_unavailable  all route targets skipped by breakers
503  dependency_unavailable    required store or bounded work unavailable
504  deadline_exceeded         overall deadline expired
==== ========================= =============================================

Errors use {error: {message, type, code}} with fixed sanitized messages.
Successful inference headers include X-Request-ID, X-Gateway-Provider, X-Cache,
X-RateLimit-Remaining-Tokens and X-Config-Generation. Headers reflect commitment
time, not final usage. Discovery does not make a provider attempt. Bearer admin
credentials are separate and cannot authenticate inference.

GET /healthz and /readyz are public; readiness checks validated startup and
SQLite/Redis availability, independent of provider health. Metrics and optional
profiling belong to the separate management listener. Admin endpoints and
remaining limits are documented in README.rst and docs/architecture.txt.
