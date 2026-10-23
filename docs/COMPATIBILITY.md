# Compatibility matrix (protocol milestone)

`POST /v1/chat/completions`, `GET /v1/models`, `GET /healthz`, `GET /readyz`.
A bearer key is required for `/v1/*`. Unknown fields, null fields, wrong types,
multiple JSON values, and unsupported combinations return HTTP 400.

| Field | Common contract | OpenAI mapping | Anthropic mapping |
| --- | --- | --- | --- |
| model | Configured alias, required | Configured model | Configured model |
| messages | 1..256 text messages; system/user/assistant; nonempty text | messages | Leading system -> system text blocks; remaining -> messages |
| stream | Boolean, default false | stream | stream |
| temperature | Finite 0..1 if supplied | temperature | temperature |
| top_p | Finite >0 and <=1 if supplied | top_p | top_p |
| max_tokens | Integer 1..configured max; omitted uses max | max_tokens | max_tokens |
| stop | Nonempty string or 1..4 nonempty strings | stop | stop_sequences |
| n | Only integer 1 | n=1 | One message |
| stream_options | Only include_usage boolean; streaming only | include_usage forced upstream | Usage translated from start/delta events |

For Anthropic, system messages must precede conversation messages, the first
conversation message must be user, and the last must be user (no prefilling).
Simultaneous temperature and top_p is rejected. Choose text-capable upstream
models that support this subset; model-specific reasoning APIs are excluded.

Responses use the gateway request ID and alias. SSE ends with `[DONE]` only
when the upstream terminal marker is received and a finish reason is known.
When requested, usage is a separate chunk with empty choices; otherwise it is
omitted. Mid-stream failures produce an error event and close without `[DONE]`.
Upstream messages, IDs, URLs and error bodies are not exposed in errors.

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | invalid_request | Invalid or unsupported input |
| 401 | invalid_api_key | Missing or invalid bearer token |
| 403 | model_not_allowed | Alias unavailable to the key |
| 429 | rate_limit_exceeded | Reserved for Redis milestone |
| 502 | upstream_failure | Transport/protocol/provider failure |
| 503 | all_providers_unavailable | Reserved for routing milestone |
| 503 | dependency_unavailable | Reserved for required store failure |
| 504 | deadline_exceeded | Overall deadline expired |

Current overload waits in the handler for an upstream slot within the overall
deadline. No unbounded application queue or per-stream producer is created.

Protocol references: [OpenAI streaming](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events),
[Anthropic streaming](https://platform.claude.com/docs/en/build-with-claude/streaming).
