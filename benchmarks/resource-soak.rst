Sustained resource and backpressure evidence
============================================

Raw report: resource-soak-2026-10-27.json. Reproduce with
python3 scripts/resource_soak.py after make build, with local Redis running.
This is a finite developer-host resource experiment, not a throughput benchmark
or proof of leak freedom. Hardware, OS, source revision and Go version are
recorded in the report.

Three independent runs each lasted at least 120 seconds with 32 concurrent
clients. Each batch canceled half the streams after first text and fully read
the other half. Completed-client connections were reused; canceled connections
were closed explicitly. The fixture used a 20ms first delay, three chunks and
100ms between chunks. Every batch verified exact mock cancellation count,
then zero active attempts, streams, work slots, shared jobs/waiters and queued
or in-flight usage. Cancellation had to be observed within one second of the
earliest client close in that batch, a conservative bound for all cancellations.

All runs passed: 15,696 streams completed and 15,696 were canceled over
360.67 seconds. Maximum batch cancellation observation was 226ms. No usage
records dropped, writes failed, or settlements failed. Quiescent goroutines
were 63..64; established TCP socket counts were 48..64. Idle HTTP connection
pools explain the retained sockets. No sustained growth beyond the regression
guards was observed.

Every five seconds, a local opt-in pprof heap read forces GC before sampling
retained heap, rather than confusing ordinary allocation sawtooth with retained
objects. RSS and TCP descriptors are observed separately. After warmup, guards
allow at most 2*concurrency+32 additional goroutines, 2*concurrency+8 additional
sockets, and 16MiB additional post-GC heap. These are bounded-work regression
guards for this workload, not production latency or memory budgets. Raw samples
are retained. No other benchmark, build or test ran during these soaks.

Ten further rounds each opened eight clients, set their receive buffers to
1KiB, and left response bodies unread. The fixture attempted 4MiB of text per
stream, under the configured total/event bounds. Downstream write deadlines
were 200ms. Every round drained active gateway and usage work within three
seconds while the client sockets were still open, then closed those clients.
This verifies that blocked consumers cannot retain active upstream streams
indefinitely. It does not claim TCP/kernel memory usage is zero or that remote
provider generation/billing has necessarily stopped.

CI runs a two-second/small-concurrency smoke version to check harness behavior;
only the recorded full runs provide the sustained evidence above. The test
harness removes only its own exact random sdk:<uuid>: Redis namespace after
its subprocesses stop. Neighbor namespaces and application/demo data remain.
