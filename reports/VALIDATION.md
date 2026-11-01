# Independent local validation: chandystore

Generated: 2026-11-01T13:53:14.920863+00:00

## What was actually executed

- **18 tests passed; 1 optional Spark test skipped.** No coverage percentage was measured.
- Real PostgreSQL 16.4, real Redis server, native C++ shared library and Uvicorn HTTP endpoints. RabbitMQ was unavailable; this is an intentionally **DEGRADED broker configuration**, not the full messaging stack.
- Chromium browser: actual /demo API interactions and WebSocket events succeeded; zero page errors; no overflow at 390px viewport. Desktop/mobile screenshots are included.
- Docker build, remote GitHub Actions CI, public backend hosting, AWS/Azure provider connectors and optional Spark were **not executed**. Configuration changes are not execution proof.

## Environment

- OS: Linux-6.18.56-x86_64-with-glibc2.34
- CPU: Intel(R) Xeon(R) Processor @ 2.50GHz; shared virtualized audit machine, not dedicated benchmark hardware.
- Python: 3.13.15
- Compiler: g++ (GCC) 11.5.0 20240719 (Red Hat 11.5.0-5); optimized C++17 dynamic library (-O3).
- Dependencies: requirements.lock. Extra audit-only tools (embedded PostgreSQL distribution, redislite, Playwright, YAML parser) are not application runtime dependencies.

## Real HTTP performance

Three trials per scenario, 200 measured requests per trial, concurrency 10, ten warmups except contention; loopback TCP and full JSON request/response. The two final project benchmarks ran sequentially. Other host activity cannot be eliminated. Statistics below are medians of per-trial results, not a fabricated pooled percentile.

| Scenario | Observed result | Boundary / caveat |
|---|---|---|
| kv | 418.0 successful requests/s | 20.53 ms median of trial P50s; 600/600 measured requests succeeded |
| locks | 388.5 successful requests/s | 24.49 ms median of trial P50s; 600/600 measured requests succeeded |
| Same-resource contention | One accepted / 199 rejected (409), in each of 3 trials | Not an availability/partition proof |

Broker state in benchmark: `unavailable`. Do not describe these results as RabbitMQ-confirmed end-to-end delivery or as pure network transit latency. The HTTP harness retains raw statuses and all timings, including rejections. Results are local synthetic measurements, not production SLAs or a reproduction of the user's Apple Silicon numbers.

HTTP benchmark source revision: `111672b3fd627af8187973f46eee334d3376d7f1`; dirty flag `True` because local evidence artifacts were present. Source implementation revisions are preserved in the supplied Git bundle. Subsequent export/browser harness and documentation-only changes do not alter the measured request paths.

## CPU-only native output

This is local function execution, **not distributed throughput**, not database commit rate, and not a network benchmark. Native runs were not repeated statistical trials. The two native builds/runs overlapped on the shared host; do not use their numbers as isolated hardware comparisons. No claim that all benchmark data fits in L1 cache is made.

```text
========================================================
   ChandyStore C++ CPU-only validation microbenchmark      
========================================================

>> CRC32 Verification Throughput: 0.98 Million entries/sec (1016.27 ns/op)
Attempted: 3000000 Successful: 3000000 Failed: 0
>> Local quorum validation + advancement:  0.70 Million commits/sec (1434.76 ns/op)
>> Local non-atomic token increment:  65.88 Million local tokens/sec (15.18 ns/op)

Consensus Final State:
  Current Term:   1
  Commit Index:   3000000
  Fencing Token:  3001000
========================================================
CRC sink: 0 (zero is possible after XOR)
```

The local quorum operation recomputes CRC when the entry changes and checks all 3,000,000 successful advancements and final commit index. This does not replicate anything to independent peers. The local token increment is non-atomic, single-threaded arithmetic; the HTTP lock path uses a PostgreSQL sequence instead.

## Safety fixes and evidence

| Fix | Implementation | Executed evidence |
|---|---|---|
| Missing-row lock races | Shared advisory transaction guard for acquire/release/write | 40 concurrent acquisitions across two gateway processes: one winner, 39 conflicts |
| Durable token allocation | PostgreSQL sequence; advance above persisted old values | Explicit migration test with token 900000; restart-check.json confirms increase after gateway process restart |
| Stale write / token omission / wrong owner | Guarded transaction requires current lease, holder, exact token | Real HTTP tests reject stale/expired/wrong-owner and missing-token writes |
| New KV key race | Same resource guard protects absent rows as well as existing rows | Concurrent stale/current write test |
| Lease/release correctness | Database clock after waits; owner/token checks | Expiry and stale release tests |
| CRC initialization | C++ call_once initialization | Native compilation, integrity and HTTP tests; not a formal concurrency proof |
| Native buffer boundaries | UTF-8 byte-length validation | HTTP rejects long multibyte key (422) |

The restart test restarts the application, **not** PostgreSQL. It is not a database crash-recovery test. The migration uses exclusive application downtime; running it during production writes is not validated. Advisory lock hash collisions may serialize unrelated resources; they do not add independent consensus.

## Files containing execution evidence

- tests.log and tests.xml: exact pytest output, including skipped Spark.
- native-benchmark.txt: terminal output from source compilation/run.
- real-http-benchmark.json and .log: trial definitions, environment, complete raw samples and statuses.
- browser-check.json/.log and browser-desktop.png / browser-mobile.png.

## Safe résumé wording

Built a PostgreSQL-backed key-value coordination prototype with transaction-scoped advisory locks, sequence-issued fencing tokens and current-lease stale-write rejection; validated two-process contention and real WebSocket updates.

Metrics may be added only with “local synthetic benchmark” and explicit boundary. Do not claim autonomous Raft, zero split-brain risk, SIMD speedups, live multi-cloud integration, production readiness or 100% code coverage. Personal reconstruction benchmarks are not employer production outcomes.
