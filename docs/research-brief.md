# Research brief: Distributed task queue (Project 05)

**Date:** 2026-07-28
**Status:** Research (pre-brainstorming)
**Target reader:** future me at brainstorming time. Assume the portfolio spec is
already read.

This brief exists to compress the landscape of task-queue prior art and design
tradeoffs into something I can argue from, not to prescribe an implementation.
Everything here is a candidate for rejection.

---

## 1. Scope tiers

The word "task queue" spans a real range. Four honest tiers, each of which
finishes as a shippable thing:

### Tier 0 — Minimum (2 weeks)

- Broker: single-process, in-memory queue plus optional Redis-backed persistence.
- Client: `submit(func, args)` → `task_id`, `get_result(task_id)` blocking or polling.
- Workers: N processes/threads pulling from the broker, executing, writing results
  back to a result store (same broker).
- Serialization: JSON for args, pickle behind a feature flag with warnings.
- Tests: unit + a small integration harness that spins broker + workers.

Resume claim at this tier is "wrote a Celery clone" — real but crowded. Not
enough for a flagship.

### Tier 1 — Middle (3-4 weeks total)

Adds the features that separate a toy from something you would tolerate in
production:

- Priority queues (multiple named queues, worker consumes with weighted round-robin
  or strict priority).
- Retry policy per task (max attempts, exponential backoff with jitter, retry-on
  specific exception classes).
- Dead-letter queue after retries exhausted, inspectable.
- Task timeouts (soft: signal; hard: process kill) with the same broker-side
  visibility.
- Scheduled tasks (`submit_at(when=...)` and `submit_after(delay=...)`) via a
  delay queue implemented on top of a sorted set / heap.
- Basic observability: a `stats()` endpoint plus structured logs.

Resume claim: "distributed task queue with retries, DLQ, priorities, scheduling,
tested to X tasks/sec." Still not flagship-differentiating on its own.

### Tier 2 — Ambitious (4-5 weeks total)

- At-least-once delivery via broker acks; explicit ack/nack from the worker;
  visibility timeout so an unacked message returns to the queue.
- Custom AMQP-ish wire protocol over TCP (framed length-prefixed messages,
  request/reply IDs, heartbeats). This is the flagship differentiator.
- Persistence layer: append-only log with periodic snapshotting (borrowed from
  the Raft KV project's WAL patterns).
- Cron-like periodic-task scheduler (a "beat" process) with leader election so
  only one beat instance is active — I can reuse the Raft implementation from
  Project 01 or use a lock in the broker itself.
- Worker autoscaling hints: broker exposes queue depth + oldest-message-age;
  a separate CLI reads them and prints "add N workers" recommendations. Not
  full autoscaling (that requires orchestrator integration).
- Prometheus /metrics endpoint. Optional lightweight web dashboard.

Resume claim: "designed and implemented a custom AMQP-like broker in Python
with at-least-once semantics; sustains X tasks/sec vs Celery+Redis Y on the
same hardware." This is defensible and differentiated.

### Tier 3 — Stretch (would push to 6 weeks)

- Effectively-once via broker-side dedup on a client-supplied idempotency key.
- Task chains / groups / chords with the coordination-primitive semantics from
  celery.canvas.
- Backpressure: broker rejects `submit` when queue over threshold; client
  applies a token-bucket rate limiter.
- Jepsen-lite fault-injection harness (kill worker, kill broker, drop packets,
  partition network).

**Recommendation for the flagship version:** Tier 2 with as much of Tier 3 as
fits in a 5th-week buffer. Tier 2 is what makes the resume line credible;
Tier 3's dedup + Jepsen-lite is the "one thing that impresses" upgrade.

---

## 2. Canonical references

Ordered roughly by how useful I expect them to be during implementation.

- **Celery source, especially `kombu`** — Kombu is the transport-abstraction
  layer; reading it exposes the actual protocol Celery uses over Redis / RabbitMQ.
  https://github.com/celery/kombu and https://github.com/celery/celery.
  `celery.canvas` for chains/groups/chords is the reference for higher-level
  composition. Beware: production-hardened code with a lot of accumulated
  compatibility layers; not always readable.

- **Sidekiq internals wiki** — Ruby, but the design docs are the clearest
  written explanation of how a simple Redis-backed queue actually works.
  https://github.com/sidekiq/sidekiq/wiki/How-Sidekiq-Works and the "Best
  Practices" pages. Mike Perham has written repeatedly about why Sidekiq's
  reliability guarantees are what they are.

- **RQ (Redis Queue)** — small, readable, Redis-only. Best "here is how to
  build one" reference for the Tier 0-1 scope. https://github.com/rq/rq.

- **Dramatiq** — the sanest modern Python task queue design. Explicit about
  its semantics (at-least-once by default), sensible middleware model.
  https://dramatiq.io/ and https://github.com/Bogdanp/dramatiq. Compare/contrast
  against Celery in the write-up.

- **arq** — asyncio-native, Redis, minimal. Interesting for the execution
  model discussion. https://github.com/samuelcolvin/arq.

- **RabbitMQ / AMQP 0-9-1 spec** — the reference for what a real broker
  protocol looks like. https://www.rabbitmq.com/tutorials/amqp-concepts.html
  and the AMQP 0-9-1 spec PDF. The RabbitMQ blog on "when messages get lost"
  is the canonical explainer for delivery-semantics tradeoffs.

- **Little's Law and elementary queueing theory** — L = λW. Useful for
  reasoning about queue depth, latency, and how many workers are needed;
  and specifically for calling out in the write-up that steady-state throughput
  is bounded by the slowest stage. Kleinrock Vol. 1 is the textbook; Neil
  Gunther's "Guerrilla Capacity Planning" is more practical.

- **Kyle Kingsbury's "You Cannot Have Exactly-Once Delivery"** —
  https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/. Combined
  with the follow-up "You Cannot Have Exactly-Once Delivery, Redux". These
  are the citations for why "exactly-once" is a marketing term and the honest
  version is "at-least-once + idempotent workers = effectively-once."

- **Jepsen** — https://jepsen.io/. Not directly applicable (target systems
  are much bigger) but the methodology of "generate operations, inject faults,
  check invariants" is what a fault-injection suite should imitate.

- **Confluent's exactly-once semantics posts (Kafka)** — the modern "how do
  we actually approximate exactly-once" reference in the streaming world.
  Kafka's approach (idempotent producers + transactional writes) is not what
  I'd copy for a task queue, but it clarifies the vocabulary.

- **AWS SQS docs on visibility timeouts** — the cleanest small-example
  writeup of how visibility-timeout-based at-least-once actually works in
  practice.

Confidence: the URLs above I've verified from prior use; the Kleinrock/Gunther
citations are from memory and should be double-checked before writing them
into a blog post.

---

## 3. Similar OSS repos and how they read

- **Celery + Kombu (Python)** — the elephant. Good: comprehensive, real
  transport abstraction, production-battle-tested. Bad as a reference: too
  many years of compatibility crust, complex canvas semantics tangled with
  transport concerns, hard to see the core. Read for the "here's what a mature
  version looks like" perspective and to steal specific patterns (e.g., how
  billiard forks workers).

- **Billiard** (Celery's fork of multiprocessing) — worth a read for how they
  actually manage worker pools, especially prefork model and pipe-based IPC.

- **RQ** — Python, Redis-only, ~5K lines. Good: the entire codebase can be
  read in a day; the design is transparent; excellent starting point to see
  "what would my Tier 0 look like." Bad: doesn't teach anything about custom
  brokers or advanced semantics — it's a thin layer over Redis lists.

- **Dramatiq** — Python, Redis/RabbitMQ. Good: opinionated modern design;
  middleware architecture is elegant; explicit semantics. Best modern
  compare/contrast target for the write-up. Bad: less ambitious than what
  I'm planning at Tier 2.

- **Huey** — Python, Redis/SQLite. Good: SQLite backend is unusual and worth
  studying for the persistence discussion (see §6). Bad: less feature-complete;
  less used.

- **arq** — Python, asyncio, Redis. Good: only asyncio-native option worth
  reading; small enough to understand end-to-end; excellent reference for
  the "how does this look if we commit to asyncio" question. Bad: Redis-only,
  doesn't cover custom broker.

- **Faust** — Python, streams over Kafka, Robinhood-originated. Not really
  a task queue; a stream processor. Good: exactly-once story over Kafka is
  worth citing. Bad: wrong shape for this project.

- **Temporal** (Go server, multi-language clients) — durable execution engine.
  Much bigger scope. Good: canonical reference for "what if tasks had a
  full-state-machine execution history"; the write-up should acknowledge it
  as the "if you wanted to go 10x further, this is what you'd build" bar.
  Bad as a reference to copy from — different problem.

- **Sidekiq (Ruby)** — Redis-backed, canonical, well-documented internals.
  Good: cleanest "simple queue done right" reference; Perham's blog posts
  are the best written explanations of design tradeoffs at this scope. Bad:
  Ruby, so translation cost.

Reading plan: RQ end-to-end for shape; Dramatiq end-to-end for modern
patterns; skim Sidekiq wiki for design rationale; dip into Celery/Kombu only
for specific questions (transport protocol details, canvas primitives).

---

## 4. Broker choice

This is the biggest architectural decision and the one that most affects the
resume claim.

**Option A — Redis-backed only.** Broker is a set of conventions on top of
Redis (lists for queues, sorted sets for delayed tasks, hashes for metadata).
This is what RQ and default Dramatiq do. Cheap; well-understood; you get
persistence and pub/sub for free.
Cost to originality: near zero — I'd be writing my ninth Celery-alike.
Cost to timeline: probably 3 weeks total.

**Option B — RabbitMQ / AMQP.** Broker is a full AMQP server; my code is a
client. This is what Celery-with-RabbitMQ does. Real acks, real routing,
real DLQ primitives.
Cost to originality: nothing new; also requires teaching interviewers about
AMQP just to understand the demo.

**Option C — Build our own broker.** A Python process listening on a TCP
port that speaks a simple length-prefixed framed protocol. Concretely:
  - accept connections, do a HELLO/version handshake
  - operations: DECLARE queue, PUBLISH msg, CONSUME (pull-based or long-poll),
    ACK, NACK, HEARTBEAT
  - persistence: append-only log per queue with fsync policy; a snapshot
    process for compaction
  - visibility timeout: in-flight messages tracked with lease expiry; unacked
    leases are returned to the queue
  - single-node only for Tier 2; multi-node with Raft (reuse Project 01) as
    stretch

Cost to timeline: adds a solid 1.5 weeks over Option A. This is the biggest
tradeoff. Cost to correctness surface: substantially higher — bugs in the
broker are silent data loss, not "worker misbehaves."

**Option D — Pluggable backend, ship two.** Interface + Redis backend +
custom broker backend. This is the honest flagship shape. Redis backend
proves the abstraction; custom backend is the differentiator. Cost:
maintenance overhead of two backends means both must have tests, but the
Redis one becomes essentially free once the interface is right.

**Recommendation:** Option D. Ship both. The write-up angle is "we built
a broker; here's how it compares to Redis on the same workload." That's a
much more interesting sentence than "we wrapped Redis."

A specific worry: if the custom broker underperforms Redis by more than
say 3x, the claim gets awkward. Redis is C, highly optimized, hard to
beat in Python. Framing has to be about semantics (explicit acks, visibility
timeouts, DLQ as first-class) not throughput. This has to be honest.

---

## 5. Delivery semantics

Three points on the spectrum, all real, all with costs:

**At-most-once.** Broker deletes message on delivery. Worker may crash before
completing the task; task is lost. Simplest semantics, useful only for
side-effect-free work you don't mind losing.

**At-least-once.** Broker delivers message; worker acks after completing;
if worker crashes or times out, message is re-delivered. Requires: (a) acks,
(b) visibility timeout / lease, (c) workers must tolerate re-execution.
Idempotency is now the worker author's problem. This is the industry default
for a reason.

**Effectively-once (a.k.a. "exactly-once" in marketing).** At-least-once
plus dedup: either broker-side (idempotency key, seen-set) or worker-side
(the worker checks an external side-effect log before doing the work).
Kingsbury's argument is that you never get true exactly-once in a distributed
system with partial failures; you get "at-least-once delivery with idempotent
effects that make duplicates invisible." Correct.

**Recommendation for flagship:** at-least-once as the default and only
guarantee. Provide an optional idempotency-key parameter that the broker
uses for dedup within a configurable window. Document explicitly that
"exactly-once" is not offered, and cite Kingsbury. This is more honest and
also more impressive than a "we have exactly-once" claim any careful
interviewer will pick apart.

Concrete requirements for at-least-once in this build:
  - Worker sends ACK only after task result is durably written to the result
    store.
  - Broker holds message with a visibility timeout; on timeout, message is
    re-enqueued.
  - Task functions are documented as needing to be idempotent, and one of
    the demo tasks explicitly demonstrates a non-idempotent bug to make
    the point.

---

## 6. Persistence

Where do queued messages live between submission and consumption?

**Redis** — durable-ish (AOF fsync policies), but you're paying a per-op
network round trip. RQ/Dramatiq default; the "safe boring choice."

**SQLite (WAL mode)** — one file, ACID, embeddable in the broker process.
Huey uses this. Good for "solo-machine deployment" scenarios; surprisingly
fast for low-contention writes. Poor for multi-broker HA.

**Custom WAL + snapshot** — the Raft-KV shape. Every PUBLISH appends to a
log; ACK appends a delete marker; periodic snapshot compacts. Full control
over fsync semantics. Highest correctness surface; most instructive to
build. Best fit for the custom-broker option (§4C).

**Postgres LISTEN/NOTIFY** — under-rated. Postgres is used as a queue in
many production stacks (Skool, some Rails shops via `queue_classic`, and
Django-Q). Trades throughput for "we already have Postgres." Wrong choice
here — pulls in a heavy dependency and doesn't fit the "systems" angle.

**Recommendation:** For the Redis backend, obviously Redis. For the custom
broker, custom WAL + snapshot, reusing patterns from Project 01. This lets
me discuss fsync-per-op vs group-commit tradeoffs in the write-up, which is
the kind of specific detail that reads well.

Fsync policy specifically: three modes exposed as config — `always` (fsync
per publish, slow, safest), `every` N ms (group commit, default), `never`
(fastest, "you accept OS-crash data loss"). This mirrors Redis's `appendfsync`
knob and gives a real benchmarking axis.

---

## 7. Worker execution model

Python's GIL forces the discussion.

**Threaded worker.** Cheap to spawn; shares memory; but GIL means CPU-bound
tasks don't parallelize. Fine for I/O-bound tasks (API calls, DB queries).

**Process worker (fork/spawn).** Real parallelism; higher memory cost;
serialization tax on results. Celery's default via billiard. Right choice
for mixed workloads; wrong choice if tasks are tiny (fork overhead dominates).

**Asyncio worker.** One event loop per worker, `n` concurrent coroutines.
Great for high-concurrency I/O-bound work. arq does this. Requires task
functions to be async; a synchronous task blocks the loop and starves the
worker. Poor fit for CPU-bound.

**"User picks per worker."** Config-driven: `worker --pool=threads|process|asyncio`.
This is what Celery does. Adds implementation complexity but is the honest
answer.

**Recommendation:** Support process (default) and thread pool workers at
Tier 2; add asyncio as a Tier 3 stretch. Explicitly document GIL behavior
and give guidance in the README.

A specific pitfall to avoid: benchmarks that use a trivial `add(a, b)` task
will make asyncio look great and process pools look terrible because the
fork cost dominates. Benchmark methodology must include realistic workloads
(a task that sleeps 100ms, a task that does 10ms of CPU) not just no-op
tasks. This ties to §11.

---

## 8. Key design decisions for brainstorming

To be argued out in the brainstorming session, not decided here. Sketching
options and default leanings:

**Client API shape.** Decorator (`@task` — Celery, Dramatiq, RQ) vs. explicit
task class (Sidekiq's `include Worker` pattern) vs. both. Decorator is more
Pythonic and more expected. Default: decorator. Class-based available for
tasks that need lifecycle hooks (setup/teardown).

**Result backend.** Same store as broker (simple, one connection to manage)
vs. separate (broker optimized for queueing, result store optimized for
lookups). Celery lets you configure both. Default: same store initially,
document that separation is easy to add.

**Monitoring/observability.** Flower-style web dashboard? Prometheus metrics?
Structured logs only? A dashboard is a lot of frontend work for marginal
resume value; Prometheus metrics are cheap and universally credible. Default:
Prometheus /metrics endpoint, structured JSON logs, a `queue inspect` CLI
subcommand. No web UI in Tier 2; possibly a very minimal read-only HTML
page in Tier 3.

**Scheduler for periodic tasks.** Cron expression parsing (croniter) vs.
simple interval (every N seconds) vs. both. Also, the scheduler needs to
be a singleton (only one instance dispatching), which requires leader
election or a broker-side "am I the beat?" lock. Default: broker-side lock
with lease renewal; simpler than Raft-leader-election and adequate for
single-broker.

**Rate limiting.** Per-task-name rate limits (max N/sec across all workers)
vs. per-worker vs. none. Real Celery rate limits are known to be quirky.
Default: token-bucket per task name enforced broker-side at dequeue time.

**Task chains / groups.** Celery's canvas: `chain(a, b, c)` runs sequentially;
`group(a, b, c)` runs in parallel; `chord` = group + callback. Powerful but
tangled with transport. Default for Tier 2: support chain only (implemented
as "on-success, submit next task"). Groups and chords are Tier 3 if time
permits — the demo pipeline (§12) will use chain.

**Backpressure.** When queue depth exceeds threshold, does `submit` block,
reject, or accept? Default: return a "queue full" error the client can
handle. Configurable soft-limit warning + hard-limit rejection.

---

## 9. Correctness testing

Standard unit + integration is table stakes. What makes this project's
correctness story credible:

**Property-based tests (hypothesis).** Encode invariants and let hypothesis
try to break them. Candidate properties:
  - Every submitted task, given no worker crashes, is executed exactly once.
  - Every submitted task, given some worker crashes, is executed at least
    once (delivery-guarantee property).
  - After N submits and eventual quiescence, the sum of executed + DLQ +
    scheduled == N.
  - Ordering property for single-consumer, single-queue: FIFO holds.

**Fault-injection tests.** A test harness that programmatically:
  - Kills a worker mid-task-execution and asserts the task eventually
    completes.
  - Restarts the broker mid-run and asserts nothing is lost.
  - Drops broker connections mid-ACK and asserts no double-execution beyond
    the at-least-once guarantee, and that retries have bounded count.
  - Delays messages (simulating slow network) and asserts visibility timeouts
    trigger correctly.

**Replay tests for the custom broker.** Given a WAL, restart the broker and
assert queue state is exactly what it was before shutdown. This is the
cheapest correctness win for a custom broker and often the buggiest area.

**Jepsen-lite.** If the custom broker gets built, a small orchestration
that spins broker + N workers + a client, runs a workload, injects faults
from a curated list, and checks invariants at the end. Not full Jepsen (no
Clojure, no linearizability checking against a formal model) — just
"generate ops, inject faults, check invariants." Recommended for Tier 2/3
and would be a real differentiator.

---

## 10. Benchmarking

Numbers I want to defensibly report:

- **Throughput.** Tasks completed per second, no-op task, saturating N
  workers. Compare across (a) our broker + our workers, (b) Redis backend +
  our workers, (c) Celery + Redis on the same hardware. Also with realistic
  tasks (10ms CPU, 100ms sleep).

- **Submission latency.** p50/p95/p99 for `submit()` under increasing
  submission rate. Interesting inflection: where does the broker's tail
  latency spike?

- **Ack latency.** Time from worker "task done" to broker "ack recorded
  durably." Sensitive to fsync policy — hence three fsync modes as an
  explicit benchmarking axis.

- **Memory per queued task.** How big is the broker's memory footprint at
  1M queued messages? This exposes serialization efficiency.

- **Recovery time.** Broker crashes at N queued messages; time to restart
  and be serving traffic again. WAL replay speed.

Methodology rules to adhere to:
  - Report hardware explicitly (CPU model, cores, RAM, disk type).
  - Report Python version and dependency versions.
  - Warmup runs discarded; ≥5 runs; report mean + stddev, not best-of.
  - Compare against Celery on the exact same host, exact same Redis, exact
    same workload. Any advantage Celery has (C extensions in kombu) is a
    real advantage and stated as such.

Claims that will be defensible: "our custom broker sustains X tasks/sec
with at-least-once semantics on hardware H; Celery + Redis sustains Y on
the same hardware; the gap is explained by Z." Claims to avoid: any
"we beat Celery by 10x" absent very specific caveats.

---

## 11. Common pitfalls

Landmines to avoid, drawn from Sidekiq/Celery mailing-list postmortems and
Kingsbury's writing:

- **Lost messages on worker crash.** The classic failure: worker pulls
  message, deletes from queue, then crashes before executing. Mitigation:
  ack-after-complete, not ack-on-receive. Belongs in the fault-injection
  suite.

- **Silent duplicate execution.** Worker completes task, then crashes
  before sending ACK; message is re-delivered; duplicate executes. This is
  correct at-least-once behavior; the pitfall is not documenting it and
  not warning users. Belongs in README loud and clear.

- **Broker restart loses in-flight state.** Workers have unacked leases;
  broker restarts; leases are gone; behavior is undefined. Mitigation:
  persist lease state to WAL, not just queue state. Under-tested in most
  toy queues.

- **Benchmarks with no-op tasks.** Measuring the framework overhead, not
  the queue. Both matter, but reporting only no-op numbers misleads.

- **No visibility into stuck tasks.** Tasks in `running` state with no
  progress and no timeout. Any admin tool must expose "how long has this
  been running?" and "kill it."

- **Scheduler drift.** Beat instance runs late, or double-runs after a
  restart. Mitigation: leader lease + idempotent scheduling.

- **Retry storms.** A poison-message task fails, retries, floods the queue.
  Mitigation: exponential backoff with jitter (not just exponential), and
  a max-attempts cap enforced broker-side.

- **Result-backend blowup.** Tasks store results forever; result store grows
  unbounded. Mitigation: result TTL config, default something short.

- **Serialization gotchas.** Pickle in a task queue is a code-execution
  vulnerability if the broker is compromised. JSON default; pickle behind
  a big warning.

---

## 12. What flagship looks like

The single sentence to earn: **"I built a distributed task queue in Python
with a custom AMQP-like broker that sustains X tasks/sec with at-least-once
delivery semantics under fault injection, vs. Celery+Redis at Y on the same
hardware, and here's the WAL/ack/visibility-timeout design that makes it
work."**

Concretely, "flagship" here means:

- Two broker backends (Redis + custom), one interface, both tested.
- At-least-once with visibility timeouts, DLQ, retries with jitter, priorities,
  scheduled tasks, and a beat scheduler with lease-based singleton election.
- Prometheus metrics + a `queue inspect` CLI.
- Property-based tests for delivery-guarantee invariants.
- Fault-injection harness that kills workers/broker mid-run.
- Benchmark numbers with methodology, comparing against Celery+Redis on the
  same host.
- Architecture doc walking through the broker protocol design decisions.

**Blog-post angles**, ranked by expected value:

1. "How I built a Redis-competitive task-queue broker in pure Python" —
   walkthrough of the wire protocol, WAL, fsync tradeoffs, benchmarks. High
   effort, high signal.
2. "Why exactly-once is a lie, and what to build instead" — the semantics
   discussion, citing Kingsbury, grounded in this project's design choices.
3. "Fault-injection testing your task queue" — Jepsen-lite methodology,
   what invariants to check, what bugs it caught. Very hireable-looking
   post.

**Demo.** A three-stage ML pipeline expressed as a Celery-style chain:
`preprocess -> embed -> index`. Uses Project 02's inference server for the
embed step, writes vectors into Project 04's vector DB for the index step.
Runs end-to-end from a single `run-demo.sh`. This is the payoff for building
Project 05 late in the sequence: it exists in service of Project 06, which
ties everything together, and the demo already stitches the earlier work
into a coherent story.

Deliberate constraint on scope creep: no web UI, no cloud deployment, no
multi-broker cluster. If any of those come up during brainstorming, they
belong in a Tier 4 "if I had another month" section of the README, not in
this project.

---

## Open questions to resolve in brainstorming

- Do we spend the budget on the custom broker or on richer semantics
  (chains/groups/chord)? Current lean: custom broker.
- Asyncio worker in Tier 2 or Tier 3? Current lean: Tier 3 (stretch) —
  process + thread pools are enough to earn the resume line.
- Beat scheduler as a separate process or a role a broker can take? Current
  lean: separate process with a broker-side lease.
- Do we ship a Docker Compose for the demo, or is a `run-demo.sh` enough?
  Current lean: Compose, since it makes the multi-worker aspect visible.
- What Python-version-locked deps become risky (e.g., croniter, hypothesis,
  pytest-asyncio)? Verify at brainstorming time.
