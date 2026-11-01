# ChandyStore: honest interview walkthrough

## 30-second explanation

PostgreSQL-backed key-value coordination prototype with fenced leases and a C++ validation library. It models selected Raft rules, but is not an autonomous Raft cluster.

I tested the real native library, PostgreSQL/Redis integration, HTTP calls and a same-origin browser demo. I report local measurements with workload and hardware context, rather than presenting CPU-loop throughput as distributed-system throughput. RabbitMQ delivery, Docker execution, optional Spark and a public backend were not validated in this run.

## Show, do not just claim

1. Start the local stack and show `/health`. Explain DEGRADED if RabbitMQ is missing.
2. Open `/demo` and show actual API JSON and WebSocket events.
3. Acquire a lease, write the matching key with its token, release, then acquire again. Attempt a stale-token write and show HTTP 409.
4. Explain why absent-row advisory locking matters: SELECT FOR UPDATE cannot lock a row that does not exist.
5. Show tests for two-process contention, token bypass, expiration and migrated persisted token values.
6. Explain why PostgreSQL authority is not an independent Raft cluster; quorum checks are local models only.

## Safe résumé wording

Built a PostgreSQL-backed key-value coordination prototype with transaction-scoped advisory locking, sequence-issued fencing tokens and stale-write rejection; added real-HTTP concurrency and WebSocket regression tests.

If quoting a number, use reports/VALIDATION.md with “local synthetic benchmark” and the test boundary. Do not claim 100% coverage, zero split-brain risk, production readiness or deployed multi-cloud operation. Do not attribute personal reconstruction benchmarks to employer production work without independent employer evidence.
