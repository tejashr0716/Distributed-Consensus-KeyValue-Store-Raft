# ChandyStore — educational local prototype

PostgreSQL-backed key-value coordination prototype with fenced leases and a C++ validation library. It models selected Raft rules, but is not an autonomous Raft cluster.

**Security boundary:** no authentication, authorization, rate limiting or public-hosting hardening. Bind to loopback only. This is not production-ready and has not been publicly deployed by this remediation.

## Start a disposable local demo

Python 3.13 and C++17 are the tested runtime family. PostgreSQL 16 and Redis are required. RabbitMQ is optional for degraded local demonstrations; unavailable broker state is reported explicitly. Spark is optional and not installed in the supplied validation run.

### Docker Compose (configuration supplied; not executed in this audit)

```bash
docker compose up --build -d
docker compose ps
# Open http://127.0.0.1:8000/demo
```

Compose binds published ports to 127.0.0.1. Its database passwords are local demo defaults, not production secrets. Initial schema runs only on an empty Docker database volume. Never use `docker compose down -v` on data you need. Image tags are not immutable digests.

### Native processes (this path was actually tested on Linux)

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./scripts/build_native.sh
export POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5432
export POSTGRES_DB=chandystore_db POSTGRES_USER=your_local_user
export POSTGRES_PASSWORD=your_local_password
export REDIS_HOST=127.0.0.1 REDIS_PORT=6379
# Create the database first, then initialize a fresh/disposable database:
psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f sql/schema.sql
uvicorn python.api.main:app --host 127.0.0.1 --port 8010
# Open http://127.0.0.1:8010/demo
```

Set `PGPASSWORD` privately for psql if needed; it does not read POSTGRES_PASSWORD. Do not commit credentials. Services in the supplied audit used isolated PostgreSQL port 55432 and Redis port 56379, not your system services. The build script emits the host's `.so` or `.dylib`; no precompiled Mac binaries are shipped.

## Test and benchmark

Start the gateway above before running the real HTTP suite. These tests and benchmarks mutate a **disposable** local database.

```bash
source .venv/bin/activate
export BASE_URL=http://127.0.0.1:8010
# In a second terminal with the same database/Redis environment:
# uvicorn python.api.main:app --host 127.0.0.1 --port 8012
export SECOND_BASE_URL=http://127.0.0.1:8012
# Explicit disposable DB URL required for the migration test (contains local credentials):
export TEST_DATABASE_URL=postgresql://your_local_user:your_local_password@127.0.0.1:5432/chandystore_db
PYTHONPATH=. pytest python/tests tests -v --junitxml=reports/tests.xml
./scripts/build_native.sh > reports/native-benchmark.txt
python benchmarks/run_real_http_benchmark.py
```

The HTTP script defaults to three trials of 200 requests, concurrency 10, ten warmups per trial (none for contention); it records successes, status counts and raw timings. Latency is complete request/response time over loopback TCP, **not network transit time alone**. Failed requests are not counted as successful throughput. Measurements are not cloud SLAs. Native numbers have zero HTTP/database/network work and must never be presented as distributed throughput.

The legacy Spark test explicitly skips without PySpark. A skipped test is not a passing test and test pass rate is not code coverage. Installing Spark additionally requires a compatible PySpark/Java combination; that path is unverified here.

## Actual dashboard vs static showcase

`/demo` is served by FastAPI and calls same-origin API/WebSocket endpoints. It shows actual response JSON, dependency status and bounded reconnect attempts. `web/index.html` is an informational static showcase only, not a backend simulation or performance measurement. Publishing that HTML to GitHub Pages does not deploy Python, C++, PostgreSQL or RabbitMQ.

WebSocket events are best-effort and **process-local**: clients connected to another gateway process do not receive them. Slow sends are bounded by a two-second timeout per client; sequential broadcasts can still delay responses with many slow clients. There is no durable replay, outbox or exactly-once delivery. SQL commits and external notifications are not one atomic operation.

## Validation and publication

See `reports/VALIDATION.md`, `reports/tests.log`, JUnit XML, native output and real HTTP JSON. CI configuration has been repaired to build Linux `.so` and run the HTTP suite, but a remote CI run was not performed. Docker and RabbitMQ delivery were not executed here. No cloud integration, crash-recovery proof or production resilience claim is implied. Local fixes must be reviewed, committed and pushed by the repository owner; public GitHub and Pages are unchanged until that happens.

## Fencing semantics and migration

- `lock_name` must equal the KV resource `key`. Database advisory transaction locks serialize acquisition, release and writes even when no row exists yet.
- PostgreSQL sequence allocates positive tokens across gateway processes. Allocation can have gaps; numbers need not be contiguous. This is single-database authority, not distributed consensus. Tokens reflect serialized allocation, not client response arrival order. Restoring an older backup or resetting sequences can violate historical monotonicity and needs operator coordination.
- A fenced write requires the current holder client ID, exact current token and unexpired held lease. The supplied client ID is not authenticated: this is correctness logic, not a security boundary.
- Once a resource has any lock record or prior fencing token, unfenced writes are rejected. Equal tokens may make multiple writes during the same active lease; the token is not an operation ordering counter or idempotency key.
- Stale, expired and wrong-owner writes/releases return 409. Acquisition does not implicitly renew an existing lease.
- Native C++ increment helper is a **single-thread local arithmetic demonstration**. API token generation uses PostgreSQL instead. It must not be advertised as atomic distributed locking.
- CRC32 detects accidental corruption; it is not cryptographic integrity. The native quorum predicate and simulated election do not send peer RPCs or elect independent nodes.

For an existing database, back up first and stop all writers. Apply the additive migration in **one transaction** (required for the advisory transaction lock):

```bash
psql -v ON_ERROR_STOP=1 --single-transaction -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f sql/migrate_fencing.sql
```

Do not run DDL upgrades concurrently with active workers. The migration adds the KV token column and advances—not rewinds—the token sequence above persisted tokens. The schema bootstrap also includes it; that is initialization, not an online migration framework.

The key/value/client UTF-8 byte limits match C++ fixed buffers (63/127/63 bytes). Long or NUL-containing native-bound fields return 422. Native APIs are not a general untrusted-buffer interface.

## Optional repeatable browser smoke test

Install Playwright separately (not an application runtime dependency), install its Chromium browser and run `node benchmarks/check_browser.cjs` with BASE_URL set to the running gateway. Set CHROMIUM_PATH if using system Chromium. The test performs actual API actions in `/demo`, requires WebSocket events, checks page errors and mobile overflow, and writes screenshots plus JSON to reports/. It uses a disposable resource and can create synthetic telemetry alerts.
