Extended fixture evidence
===========================

The extended load reports retain all raw repetitions, measured errors, warmup
errors, CPU/RSS, sampled heap/goroutines and accounting-loss counters. Three
2-second repetitions cover direct and gateway JSON/SSE at concurrency 1, 10,
100 and 500. Input/output payload tests use 32KiB prompts and 32KiB output
(eight 4KiB chunks) at concurrency 10/100. Cache-hit, unique cache-miss and seeded
fallback measurements are separate. Coalescing uses three synchronized
100-caller batches. The release_load.py CLI reproduces each scenario.

Configuration: 512 upstream/stream slots, token capacity 1e9 and refill 1e6/s,
SQLite queue 10,000, and the sample's request/idle/write/shutdown timeouts.
Default fixture delays are 20ms first response and 30ms/chunk. Provider usage
is synthetic fixture usage; payload bytes are independent of reported tokens.
Cache reuse uses isolated temporary opt-in keys. Cache misses append a unique
input suffix; hits use identical primed input. Warmups are excluded from rate
and latency samples but their errors are retained. The latest harness drains
warmup and active work before taking final ownership counters.

SQLite contention holds a real BEGIN IMMEDIATE writer lock for one second,
while the same inference workload continues. Warmup/traffic still authenticate
with WAL reads. Loss counters include failed usage writes; this is intentional
and independent of request success. Failure/backpressure behavior is also
covered by deterministic tests.

Stream-drop measurements use a 1ms breaker observation window to isolate stream
failure rather than accidentally measuring fast breaker rejections. They run
at most one second per repetition to avoid overwhelming the host's ephemeral
ports with intentionally broken HTTP connections. Each reported failure must
be an actual SSE error, not an HTTP breaker rejection or client state error.
Breaker transitions themselves are tested with fake clocks and separately
observed in the normal seeded-fallback matrix. Shared coalescing uses a 300ms
fixture first delay so callers overlap; each batch must own one provider call.

Recorded results
------------------

Host: Apple M4, 10 logical CPUs, 16GiB, macOS 26.6.2, Go 1.27.3, Python 3.14.7,
Redis 7.4.11. The source commits, binary/driver hashes and all stage data are
inside the JSON reports. SQLite and Redis settings used by the workloads are
explicit in scripts/release_load.py and configs/demo.json. Native gateway and
fixture processes share the developer host with Docker Redis and idle demo
monitoring. This is closed-loop fixture evidence, not a capacity guarantee.

* extended-load-2026-10-27.json: 93 stages, including concurrency 500, larger
  payloads, separate cache hits/misses, seeded fallback and SQLite contention.
* release-faults-2026-10-27.json: six isolated stream-drop stages and three
  coalesced batches. The 100-caller batches each returned one MISS and
  99 COALESCED, with one upstream attempt.
* experiments/extended-interrupted.json: original capture, including superseded
  fault stages. It is retained as a failed experiment, not accepted stream-drop
  evidence. Opening breakers made errors return quickly; the generator then
  exhausted local ephemeral ports. Corrected captures above isolate error types
  and avoid that reconnect storm. No OS socket settings were changed.

The accepted non-fault matrix completed 468,148 requests. At concurrency 500,
57 warmup requests returned transient HTTP 502; measured requests at that level
completed without errors. These startup failures are part of the result and
prevent a claim of error-free burst admission at concurrency 500 on this host.
The other non-fault stages reported no request errors. No settlement errors
occurred. The SQLite lock deliberately caused 27 failed/dropped usage records
while 2,450 inference requests completed successfully in that scenario. Other
accepted non-fault scenarios reported no usage drops/write failures.

Cache-hit stages returned 83,074 completions with zero additional upstream
attempts. Cache-miss stages completed 27,381 requests with 27,381 attempts.
These figures count only the measured stages, with the documented warmup
ownership limits in the original capture. Fault stages report errors rather
than inventing successful completion rates; successful-response latency/TTFT
percentiles are empty in an all-failure stage.

CPU/generator saturation, connection bursts, timer scheduling and SQLite/Redis
costs can limit results. Inspect each repetition rather than subtracting
percentiles into an overhead distribution. No numerical production performance
budget, exactly-once execution, leak freedom or billing durability is claimed.
