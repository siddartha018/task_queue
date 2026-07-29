# Distributed Task Queue (Python)

A distributed task queue built from primitives: a broker that owns the
job-state machine, an HTTP wire protocol over FastAPI, a client SDK that
knows about idempotency and dependencies, and a worker that speaks the
protocol and enforces per-job timeouts. Written in Python 3.11+ with
strict typing.

Design decisions and the reference-project comparison
(Celery / Sidekiq / RQ / Faktory / Prefect) are recorded in
[docs/research-brief.md](docs/research-brief.md). This build corresponds to
the brief's **Tier 1/2 hybrid**: priorities + retries with exponential
backoff + DLQ + scheduled delivery + at-least-once semantics + Prometheus
metrics. Custom AMQP-style TCP wire protocol (Tier 2 stretch) is
deliberately swapped for HTTP so this project drops into the MLOps pipeline
(Project 06) unchanged.

---

## What's in the box

- **Priority queues.** Any named queue; lower priority number = served
  first. Ties broken by submission time.
- **At-least-once delivery.** Workers `claim` → run → `ack`/`nack`. A
  visibility deadline arms on claim; if a worker dies before ack/nack, the
  job is re-queued on the next broker tick.
- **Retry with exponential backoff and jitter.** Configurable per policy
  (max_attempts, initial_backoff_s, backoff_multiplier, max_backoff_s,
  jitter_s). Dead-letters after the last failure.
- **Scheduled tasks.** `run_at=<unix_ts>` or `delay_s=…` — sits in a min-heap
  until due, then moves into the queue.
- **Task dependencies.** `depends_on=[job_id]` — a dependent job is held
  until every dependency reaches SUCCESS. If a dependency dies, downstream
  jobs die with it.
- **Idempotency keys.** A second submit with the same `idempotency_key`
  returns the first job_id — safe retries on the client side.
- **Per-job timeouts.** Enforced by the worker via
  `ThreadPoolExecutor.result(timeout=)`; caught and reported as `nack`.
  The broker's visibility deadline is the second line of defence.
- **HTTP wire protocol.** Small JSON API over FastAPI. Any language with an
  HTTP client can be a client.
- **Prometheus `/metrics`.** Per-queue submit / complete / failure / dead
  counters; queue depth, scheduled depth, running gauges.
- **CLI.** `taskq serve`, `taskq worker`, `taskq submit`, `taskq stats`,
  `taskq dead`, `taskq show`, `taskq wait`.
- **19 tests, all passing** in ~2.6s.

---

## Architecture

```
                       +------------------------+
                       |     Client (SDK)       |
                       |  taskq.TaskQueueClient |
                       +----+----------------+--+
                            |                |
                        POST /jobs         GET /jobs/{id}
                            v                v
   +-----------------------------------------------------------+
   |                     Broker (FastAPI)                       |
   |                                                            |
   |   +-----------------+   +------------------+   +--------+  |
   |   | per-queue heaps |   | scheduled heap   |   |  DLQ   |  |
   |   | (min by priority|   | (min by run_at)  |   |        |  |
   |   |  then FIFO)     |   +------------------+   +--------+  |
   |   +-----------------+                                       |
   |          ^                       |                          |
   |          | promote on tick       |                          |
   |          +-----------------------+                          |
   |                                                             |
   |   claim -> RUNNING (visibility_deadline armed)              |
   |   ack   -> SUCCESS, release dependents                      |
   |   nack  -> retry (SCHEDULED w/ backoff) or DEAD             |
   |                                                             |
   |   /metrics (prometheus_client)                              |
   +----------------------------+--------------------------------+
                                |
                            POST /jobs/claim
                            POST /jobs/{id}/ack
                            POST /jobs/{id}/nack
                                v
                       +--------+-------+
                       |     Worker     |
                       |   (long-poll)  |
                       |  registered    |
                       |  Python tasks  |
                       |  timeouts via  |
                       |  ThreadPool    |
                       +----------------+
```

- Every state transition happens inside a single `asyncio.Lock` in the
  broker. Handlers run in worker processes, not the broker.
- Visibility timeout expiry is checked on every "promote due scheduled"
  tick (default: 100 ms). An expired job is re-queued immediately (the
  delay already happened) rather than adding a fresh backoff.

### Job state machine

```
                    submit()
                       |
                       v
   depends_on unmet -> waiting  ---(all deps ack'd)--->  PENDING
                                                            |
              run_at future ->  SCHEDULED  --tick+due-->    |
                                                            v
                            claim() (attempts++, visibility_deadline armed)
                                                            |
                                                            v
                                                        RUNNING
                                                        /       \
                                                    ack()      nack()
                                                       |         |
                                                       v         v
                                                    SUCCESS   attempts < max?
                                                                / \
                                                            YES     NO
                                                             |       |
                                                       SCHEDULED    DEAD
                                                    (backoff+jitter)  |
                                                                      |
                                                          waiting-on-me → DEAD
```

---

## Repository layout

```
prior-projects/05-task-queue/
+- pyproject.toml
+- .gitignore
+- README.md                            (this file)
+- docs/research-brief.md               (design-space survey)
+- src/taskq/
|  +- __init__.py                       (public API)
|  +- models.py                         (Job, JobSpec, JobStatus, RetryPolicy)
|  +- broker.py                         (state machine, priority heaps, DLQ)
|  +- server.py                         (FastAPI HTTP surface + Prometheus)
|  +- client.py                         (httpx-based SDK)
|  +- worker.py                         (@register decorator, run loop)
|  +- cli.py                            (typer CLI)
+- examples/
|  +- tasks.py                          (add / slow_add / boom / flaky)
+- tests/
   +- conftest.py                       (FakeClock, broker fixture)
   +- test_broker.py                    (12 broker tests)
   +- test_retry_policy.py              (3 backoff tests)
   +- test_server_and_worker.py         (3 end-to-end tests with real server)
```

---

## Quickstart

### 1. Install

```bash
cd prior-projects/05-task-queue
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Run the tests

```bash
pytest -q
```

Expect **19 passed** in a couple of seconds. That covers the broker state
machine (priority, retries, dead-lettering, scheduled tasks, dependencies,
idempotency, cancel, visibility-timeout requeue), the retry policy
(monotonic, capped, jittered), and an end-to-end pass that spins up the
real FastAPI server + a real Worker.

### 3. Start the broker

```bash
taskq serve --host 127.0.0.1 --port 8000
```

### 4. Run a worker

Point it at a module containing `@register` decorators:

```bash
PYTHONPATH=. taskq worker --tasks examples.tasks --url http://127.0.0.1:8000
```

### 5. Submit a job

```bash
taskq submit add --args '{"a": 2, "b": 3}' --url http://127.0.0.1:8000
# -> {"job_id": "abcd..."}
taskq wait abcd... --url http://127.0.0.1:8000
```

Or from Python:

```python
from taskq import TaskQueueClient, JobSpec

with TaskQueueClient("http://127.0.0.1:8000") as c:
    job_id = c.submit(JobSpec(task="add", args={"a": 2, "b": 3}))
    result = c.wait(job_id, timeout_s=10).result
```

---

## HTTP wire protocol

All bodies are JSON.

| Verb | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job. Body = `JobSpec`. Returns `{"job_id": "..."}`. |
| `GET`  | `/jobs/{id}` | Fetch a job's server-side record (status, attempts, result, error). |
| `POST` | `/jobs/claim` | Worker: claim the next runnable job. Body = `{queues: [...]}`. `204` if idle. |
| `POST` | `/jobs/{id}/ack` | Worker: mark success. Body = `{result: ...}`. |
| `POST` | `/jobs/{id}/nack` | Worker: mark failure. Body = `{error: "..."}`. |
| `POST` | `/jobs/{id}/cancel` | Cancel a non-terminal job. |
| `GET`  | `/dead` | List dead-letter jobs. |
| `GET`  | `/stats` | Broker stats snapshot. |
| `GET`  | `/metrics` | Prometheus exposition. |
| `GET`  | `/healthz` | Liveness. |

Fields in `JobSpec`:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `task` | string | required | Registered task name. |
| `args` | object | `{}` | JSON-serializable kwargs. |
| `queue` | string | `"default"` | Named queue. |
| `priority` | int 0..9 | `5` | Lower = served first. |
| `idempotency_key` | string \| null | `null` | Dedup token. |
| `max_attempts` | int \| null | policy's | Override retries for this job. |
| `timeout_s` | float | `300.0` | Per-attempt worker timeout. |
| `run_at` | float \| null | `null` | Unix epoch seconds; delay until then. |
| `retry_policy` | RetryPolicy \| null | `null` | Override backoff params. |
| `depends_on` | list[string] | `[]` | Job IDs that must SUCCESS first. |

---

## Semantics you can defend

| Property | How it's implemented |
|---|---|
| **At-least-once delivery** | Manual ACK. `claim` opens a visibility window; if the worker dies before `ack`/`nack`, `promote_due_scheduled` re-queues the job on the next tick (100 ms cadence). |
| **Priority is strict within a queue, and across queues in a single `claim` call** | Each queue is its own min-heap keyed on `(priority, submitted_at, seq)`. Multi-queue claim picks the queue with the smallest heap-top tuple. |
| **FIFO within same priority** | Heap key includes `submitted_at` then a monotonic `seq`, so equal-priority jobs are served oldest-first. |
| **Exponential backoff with jitter** | `RetryPolicy.backoff_for_attempt(n, rand) = min(init * mult^(n-1), max) + rand * jitter_s`. Tested for monotonicity, cap, and bounded jitter. |
| **Dead-letter after exhaustion** | On the last-attempt nack, the job moves to `DEAD`, becomes visible via `GET /dead`, and any dependents die with it. |
| **Idempotency** | `idempotency_key` maps to the first `job_id`; a second submit with the same key is a lookup, not a new job. |
| **Dependencies (fan-in)** | A dependent job is held in `_waiting_on`; each dep's ack removes an entry; the last one enqueues the dependent. Dep death cascades. |
| **Per-job timeouts** | Worker submits the callable to a `ThreadPoolExecutor` and calls `future.result(timeout=spec.timeout_s)`; on timeout it nacks. The broker's visibility deadline (= timeout + safety margin) is the fallback if the worker itself dies. |
| **Cancel** | Only cancels jobs not yet terminal. A cancelled job still in a heap becomes a stale entry that `claim` skips. |

---

## Testing

**19 tests, all passing** across three suites:

| Suite | Count | What it covers |
|---|---|---|
| `test_broker.py` | 12 | Submit + claim, ack marks success, priority ordering across three levels, FIFO within same priority, retry-until-dead, retry-then-success, **visibility timeout requeues on worker death**, idempotency returns same id, `run_at` scheduling, dependency gating, **dead upstream cascades to dependents**, stats snapshot, cancel. |
| `test_retry_policy.py` | 3 | Backoff monotonic, capped at max, jitter bounded within `[base, base + jitter_s]`. |
| `test_server_and_worker.py` | 3 | Real `uvicorn` server + real `Worker` on ephemeral port: end-to-end success, retry-then-dead, unknown-task nack. |

Broker tests use a `FakeClock` fixture so retry deadlines are advanced
deterministically without wall-clock sleeps.

---

## Metrics

Prometheus exposition on `/metrics`:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `taskq_submitted_total` | Counter | `queue` | Jobs submitted. |
| `taskq_completed_total` | Counter | `queue` | Jobs ack'd successfully. |
| `taskq_failed_total` | Counter | `queue` | Job nacks (any attempt). |
| `taskq_dead_total` | Counter | `queue` | Nacks that exhausted retries. |
| `taskq_queue_depth` | Gauge | `queue` | Runnable-heap depth. |
| `taskq_scheduled_depth` | Gauge | — | Scheduled/delayed depth. |
| `taskq_running` | Gauge | — | Jobs currently claimed. |

Scrape `/metrics` at 15s or so; graph `taskq_queue_depth` vs
`taskq_completed_total` to spot workers falling behind, and `taskq_dead_total`
by queue as the "poison messages" alarm.

---



## Status

| Milestone | State |
|---|---|
| Broker state machine (priority + FIFO within priority) | Done |
| Retries with exponential backoff + jitter + max cap | Done |
| Dead-letter queue with inspection endpoint | Done |
| Scheduled / delayed tasks | Done |
| Task dependencies with cascading failure | Done |
| At-least-once delivery via visibility timeout | Done |
| Idempotency keys | Done |
| Per-job timeouts (worker + broker enforcement) | Done |
| HTTP wire protocol + client SDK | Done |
| Worker with @register decorator, ThreadPool concurrency | Done |
| Typer CLI: serve, worker, submit, stats, dead, show, wait | Done |
| Prometheus /metrics endpoint | Done |
| 19-test suite (broker + policy + end-to-end) | Done |
| Custom AMQP-style TCP wire protocol (research brief Tier 2 stretch) | Not attempted |
| Cron-style periodic scheduler ("beat" with leader election) | Not attempted |
| Redis or Postgres persistence backend | Not attempted |
| Jepsen-lite fault-injection harness | Not attempted |

---

## References

Full design-space survey, reference-project comparison (Celery / Kombu /
Sidekiq / RQ / Faktory / Prefect / arq), and pitfall catalogue:
[docs/research-brief.md](docs/research-brief.md).
