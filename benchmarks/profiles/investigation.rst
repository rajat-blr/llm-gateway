Strict JSON allocation investigation
====================================

The original validator walked encoding/json.Decoder.Token before decoding the
typed request with DisallowUnknownFields. This preserved duplicate/null/depth
checks but allocated large decoder buffers and materialized every string value
in a 32KiB prompt. The original remains a differential oracle in
internal/gateway/json_performance_test.go.

The replacement first calls json.Valid, then scans already-valid bytes to
check nulls, nesting and duplicate keys. It skips string values without copying
them and decodes only escaped/invalid-UTF8 object keys, preserving equivalence
with encoding/json's key normalization. Typed decoding still enforces unknown
fields and request types. The bounded nesting check and upstream capability
validation remain in place.

Five 500ms repetitions on Apple M4 / darwin-arm64 / Go 1.27.3:

==================== ===================== =====================
Metric               Original median       Replacement median
==================== ===================== =====================
128-byte ns/op       1,560                 1,352
128-byte bytes/op    2,548                 1,265
128-byte allocs/op   43                    21
32KiB ns/op          85,285                73,326
32KiB bytes/op       329,607               164,770
32KiB allocs/op      60                    30
==================== ===================== =====================

The measured improvement is about 14% less parse time and 50% fewer allocated
bytes for the large-prompt microbenchmark. It is not a general gateway speedup.
The raw repetitions are strict-json-before.txt / strict-json-after.txt.
The current benchmark contains both implementations for reproduction::

    go test ./internal/gateway -run '^$' -bench BenchmarkStrictJSON -benchmem -benchtime=500ms -count=5

Controlled full-gateway profiles use the same workload: eight persistent JSON
clients, 32KiB prompt, 5ms fixture first delay, no cache, 10-second CPU profile
with 11 seconds of traffic. Both have zero request errors, usage drops, write
failures and settlement errors. These sampled profiles include Go/runtime,
HTTP, Redis and SQLite costs. End-to-end throughput was 1094.74 versus 1060.24
requests/second; that single profiled pair does not demonstrate an end-to-end
speedup. Post-GC sampled live heap can vary independently of allocation rate.
See controlled-before.json / controlled-after.json for complete measurements.

Raw CPU, live-heap and allocation profiles are checked in as *.pprof, with
human-readable top tables. They contain stack samples, not heap object contents.
Use the matching original binary (source ce9ad56) for original symbolization;
replacement source is the strict-JSON change in commit 412667e. Build IDs/hashes
are recorded in JSON; Go VCS stamps differ for working-tree builds. Most top
summaries can be inspected without a binary::

    go tool pprof -top benchmarks/profiles/controlled-after-cpu.pprof
    go tool pprof -top -sample_index=alloc_space benchmarks/profiles/controlled-after-allocs.pprof
    python3 scripts/profile_gateway.py --output tmp/profiles/new

An earlier zero-delay, 32-client profiling experiment exceeded SQLite's writer
capacity and produced visible usage drops. It motivated the controlled rate
above and the separate contention/overload measurements. The gateway keeps
serving when the documented bounded best-effort usage queue loses records;
its loss counters must be inspected, not interpreted as billing durability.

Correctness checks include the complete race/SDK suites, differential seeds
for escaped equivalent keys, invalid UTF-8, nulls and depth bounds, and two
30-second differential fuzz runs (340,268 and 272,997 executions). CI repeats
bounded differential fuzzing. No parser mismatch was found in these runs;
finite fuzzing is evidence rather than an exhaustive proof.
