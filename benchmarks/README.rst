Local baseline method
======================

Run from a Python environment with examples/requirements.txt installed::

    docker compose up -d --wait redis
    make baseline

The default run measures concurrency 1, 10 and 100, JSON and SSE, with three
2-second repetitions for both direct mock and gateway paths. The harness owns
its gateway, primary/backup mocks and temporary SQLite database. Redis uses a
unique test prefix. It never calls a paid provider. Allow builds/tests to
finish before collecting results. JSON output defaults to tmp/baseline.json.
A smaller CI smoke run validates the harness and is uploaded as an artifact;
it does not establish a performance threshold.

The workload sends identical OpenAI text request bodies through persistent
HTTP/1.1 connections. Each worker warms its connection once, then uses a closed
loop without think time until the stage deadline; in-flight requests finish.
Direct/gateway order alternates by repetition. Fixture delays are 20ms before
headers and 30ms before each of three text chunks. Both paths verify complete
fixture text and successful SSE termination. Reuse is disabled on the bootstrap
key. Admissions have a generous token budget and 512 work/stream slots so these
measurements exercise auth, Redis accounting, adapters and asynchronous SQLite
usage without intentionally queueing at the gateway concurrency limit.

The report records per-run nearest-rank latency/TTFT percentiles, errors,
observed completions per second, CPU deltas and final RSS for gateway/mock,
generator CPU and process-wide peak RSS, 250ms sampled runtime peaks, and usage
loss/accounting counter deltas. First text means actual nonempty text, excluding
role/control events. CPU deltas use ps process accounting with OS resolution.
Generator peak RSS is cumulative across stages. Sampled peaks can miss brief
spikes; final RSS is not peak RSS. Gateway source revision, binary/driver hashes,
Go/Python/Redis versions, host CPU/memory and resource bounds accompany results.

After load stages, ten rounds of 32 concurrent streams disconnect every other
client after first text. Each round verifies matching mock cancellations and
that active attempts, streams, work slots, shared executions/waiters and the
usage queue/in-flight write return to zero within five seconds. Quiescent heap
and goroutine values are reported without forcing GC or closing upstream idle
connections. This is a bounded cancellation/resource check, not a long-duration
leak proof. Shutdown, cache/coalescing, dependency outages and provider failures
are separately covered by integration/acceptance tests.

Interpretation limits
----------------------

All processes and Docker Redis share one developer host, alongside the idle
reviewer/monitoring stack and other local applications. This is local fixture
evidence rather than an isolated lab measurement. Closed-loop throughput is
not a maximum offered-rate capacity result. The Python generator and mock can
become bottlenecks; inspect their CPU alongside the gateway. No concurrency-500,
large-payload, cache, fallback, long-soak or production-provider performance is
claimed. Subtracting independently measured percentile values does not yield
a per-request overhead percentile. Follow-up optimization requires profiles
and the same workload before/after, rather than conclusions from a single run.

Recorded developer-host run (October 26, 2026)
----------------------------------------------

Raw data: baseline-2026-10-26-arm64.json, gateway revision 8bb5f1a. Apple M4,
10 logical CPUs, 16GiB memory, macOS 26.6.2, Go 1.27.3, Python 3.14.7 and
Docker Redis 7.4.11. No builds or tests ran during this measurement. The existing
reviewer/monitoring services remained idle apart from normal scrapes.

Across 36 stages: 65,235 verified completions, zero request errors, and zero
usage drops, failed writes or settlement errors. Measured gateway stages owned
35,264 client requests and exactly 35,264 upstream attempts. The soak verified
160 upstream cancellations from 320 streams across ten rounds. Every round
returned active work/streams/attempts/shared jobs/waiters/usage to zero. Sampled
quiescent goroutines went from 184 after load to 47 and stayed at 47 for the
remaining nine rounds; heap varied with GC. These observations do not establish
long-duration leak freedom.

Observed completion rates below show the full range across three repetitions;
latencies are the median of the three per-run percentiles, not pooled samples.

=========== ====== ================== =================== ================= ==================
Concurrency Mode   Direct req/s range Gateway req/s range Direct p50/p95 ms Gateway p50/p95 ms
=========== ====== ================== =================== ================= ==================
1           json   45.8..46.4         41.2..42.0          21.64 / 22.04     23.56 / 26.54     
1           stream 8.5..8.7           7.4..8.5            116.65 / 119.53   118.32 / 143.41   
10          json   430.4..434.7       399.4..401.1        23.04 / 25.45     24.90 / 27.70     
10          stream 79.2..83.9         80.6..81.0          118.67 / 126.99   123.60 / 127.70   
100         json   2179.5..4461.0     4362.8..4560.7      22.14 / 26.11     21.54 / 23.76     
100         stream 641.2..832.6       743.7..814.1        116.10 / 136.83   123.30 / 144.44   
=========== ====== ================== =================== ================= ==================

The large spread in direct concurrency-100 runs, and overlapping or reversed
rankings, show why these measurements cannot establish an overhead or speedup
claim. Maximum measured mean CPU was about 2.03 gateway cores, 0.44 fixture
cores and 0.71 generator cores. These do not rule out scheduling, Docker/Redis,
I/O or generator bottlenecks. Those baseline variations motivated controlled
profiling and the specific before/after experiment recorded below.

Extended release evidence (October 27, 2026)
---------------------------------------------

The original baseline measurements above remain unchanged. The completed release work adds:

* extended-method.rst and extended-load-2026-10-27.json: larger inputs/outputs,
  concurrency through 500, cache/fallback/SQLite measurements and observed limits.
* release-faults-2026-10-27.json: isolated stream drops and 100-way coalescing.
* profiles/investigation.rst: checked-in CPU/heap/allocation profiles and a
  differential-tested strict-JSON optimization with raw before/after repetitions.
* resource-soak.rst and resource-soak-2026-10-27.json: three sustained 120-second
  cancellation runs and ten unread large-response rounds with finite write budgets.
* experiments/: interrupted fault-load and zero-delay overload evidence, retained
  to make generator saturation and documented usage loss visible.

Run make release-evidence with the SDK Python environment active to repeat the
extended matrix and soaks. --only selects individual release_load.py scenarios.
Source/configuration, raw samples, warmup errors and measurement limits are
part of the result; no general gateway speedup or production capacity is claimed.
