"""In-process broker: the single source of truth for job state.

Semantics (see docs/research-brief.md):
- **At-least-once delivery.** A worker `claim`s a job; the job enters RUNNING
  with a visibility deadline. If the worker doesn't `ack` (success) or
  `nack` (failure) before the deadline expires, the job is re-queued and
  another worker may claim it. Handlers must be idempotent.
- **Priority scheduling per queue.** Each named queue is a min-heap keyed on
  (priority, submitted_at). Lower priority number = served first.
- **Delayed / scheduled tasks.** Jobs with `run_at` in the future or with a
  computed backoff sit in a single scheduled min-heap keyed on next_run_at
  until due, then move into their queue.
- **Retries.** On nack, if attempts < max, compute the next backoff, mark
  the job SCHEDULED, and place it back on the scheduled heap. On exhaustion,
  move to DEAD.
- **Dependencies.** `depends_on = [job_id, ...]` — a dependent job is held
  in a `waiting` set until every dependency has SUCCESS status. On the last
  dependency success, we release it into its queue.
- **Idempotency.** `idempotency_key` is a client-visible dedup token: a
  second submit with the same key returns the first job_id.

Thread-safety: the broker is protected by an asyncio.Lock. All state
mutations happen under the lock; job execution happens in workers, not
here.
"""
from __future__ import annotations

import asyncio
import heapq
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from .models import Job, JobSpec, JobStatus, RetryPolicy, StatsSnapshot


@dataclass(order=True)
class _QEntry:
    # (priority, submitted_at, seq) -> job_id
    priority: int
    submitted_at: float
    seq: int
    job_id: str = field(compare=False)


@dataclass(order=True)
class _SchedEntry:
    when: float
    seq: int
    job_id: str = field(compare=False)


class Broker:
    def __init__(
        self,
        *,
        default_visibility_s: float = 30.0,
        default_retry: RetryPolicy | None = None,
        clock: Any = time,
        rng: random.Random | None = None,
    ) -> None:
        self.default_visibility_s = default_visibility_s
        self.default_retry = default_retry or RetryPolicy()
        self._clock = clock
        self._rng = rng or random.Random(0xC0FFEE)

        self._lock = asyncio.Lock()
        self._jobs: dict[str, Job] = {}
        self._by_idem: dict[str, str] = {}
        self._queues: dict[str, list[_QEntry]] = defaultdict(list)
        self._scheduled: list[_SchedEntry] = []
        self._waiting_on: dict[str, set[str]] = {}     # job_id -> set of unfinished deps
        self._dependents_of: dict[str, set[str]] = defaultdict(set)  # dep_job_id -> {waiting_child}
        self._dead: list[str] = []
        self._running: set[str] = set()
        self._seq = 0

        # Stats
        self._total_submitted = 0
        self._total_completed = 0
        self._total_failed = 0
        self._total_dead = 0

    def now(self) -> float:
        return float(self._clock.time())

    async def submit(self, spec: JobSpec) -> str:
        async with self._lock:
            if spec.idempotency_key and spec.idempotency_key in self._by_idem:
                return self._by_idem[spec.idempotency_key]

            job_id = uuid.uuid4().hex
            now = self.now()
            job = Job(
                id=job_id,
                spec=spec,
                status=JobStatus.PENDING,
                submitted_at=now,
            )
            if spec.run_at is not None and spec.run_at > now:
                job.status = JobStatus.SCHEDULED
                job.next_run_at = spec.run_at

            self._jobs[job_id] = job
            if spec.idempotency_key:
                self._by_idem[spec.idempotency_key] = job_id
            self._total_submitted += 1

            # Dependencies?
            unfinished: set[str] = set()
            for dep in spec.depends_on:
                dj = self._jobs.get(dep)
                if dj is None:
                    # Referencing a job we don't know: treat as unresolvable -> dead.
                    job.status = JobStatus.DEAD
                    job.error = f"depends on unknown job {dep}"
                    self._dead.append(job_id)
                    self._total_dead += 1
                    return job_id
                if dj.status != JobStatus.SUCCESS:
                    unfinished.add(dep)
                    self._dependents_of[dep].add(job_id)
            if unfinished:
                self._waiting_on[job_id] = unfinished
                # It stays PENDING (or SCHEDULED) but is not on any runnable heap.
                return job_id

            self._enqueue(job)
            return job_id

    def _enqueue(self, job: Job) -> None:
        self._seq += 1
        if job.status == JobStatus.SCHEDULED:
            when = job.next_run_at or self.now()
            heapq.heappush(self._scheduled, _SchedEntry(when=when, seq=self._seq, job_id=job.id))
        else:
            heapq.heappush(
                self._queues[job.spec.queue],
                _QEntry(
                    priority=job.spec.priority,
                    submitted_at=job.submitted_at,
                    seq=self._seq,
                    job_id=job.id,
                ),
            )

    async def promote_due_scheduled(self) -> int:
        """Move scheduled jobs whose run_at has arrived into their queues.

        Also sweeps expired visibility deadlines for RUNNING jobs and re-queues
        them (at-least-once redelivery on worker death). Returns the number of
        jobs moved into runnable state.
        """
        moved = 0
        async with self._lock:
            now = self.now()
            # Promote scheduled -> queue.
            while self._scheduled and self._scheduled[0].when <= now:
                entry = heapq.heappop(self._scheduled)
                job = self._jobs.get(entry.job_id)
                if job is None or job.status not in (JobStatus.SCHEDULED, JobStatus.FAILED):
                    continue
                job.status = JobStatus.PENDING
                job.next_run_at = None
                self._enqueue(job)
                moved += 1

            # Reclaim expired visibility deadlines.
            expired = []
            for jid in list(self._running):
                job = self._jobs.get(jid)
                if job is None:
                    self._running.discard(jid)
                    continue
                if job.visibility_deadline is not None and job.visibility_deadline <= now:
                    expired.append(jid)
            for jid in expired:
                job = self._jobs[jid]
                self._running.discard(jid)
                # Visibility-timeout is an implicit nack. Unlike a real nack,
                # the "delay" already happened (in the form of the missed
                # deadline), so we requeue immediately rather than tacking on
                # a fresh exponential backoff.
                await self._nack_locked(job, "visibility timeout expired", immediate_requeue=True)
                moved += 1

        return moved

    async def claim(self, queues: list[str] | None = None) -> Job | None:
        """Claim the next runnable job. Returns None if nothing is runnable.

        Picks the highest-priority job among the specified queues; ties broken
        by submitted_at, then submission sequence.
        """
        async with self._lock:
            now = self.now()
            candidates: list[tuple[_QEntry, str]] = []
            names = queues or list(self._queues.keys())
            for q in names:
                if self._queues[q]:
                    candidates.append((self._queues[q][0], q))
            if not candidates:
                return None
            candidates.sort(key=lambda t: (t[0].priority, t[0].submitted_at, t[0].seq))
            _, best_q = candidates[0]
            entry = heapq.heappop(self._queues[best_q])
            job = self._jobs.get(entry.job_id)
            if job is None or job.status not in (JobStatus.PENDING,):
                # Stale entry (e.g., cancelled). Recurse to pick something else.
                return await self._claim_recur_locked(queues)
            job.status = JobStatus.RUNNING
            job.attempts += 1
            job.started_at = now
            job.visibility_deadline = now + job.spec.timeout_s + self.default_visibility_s
            self._running.add(job.id)
            return job

    async def _claim_recur_locked(self, queues: list[str] | None) -> Job | None:
        # Called from claim() when a stale queue entry is skipped. We're still
        # inside the lock (we hold self._lock via the outer async with), so we
        # do the recursion inline without re-acquiring.
        now = self.now()
        candidates: list[tuple[_QEntry, str]] = []
        names = queues or list(self._queues.keys())
        for q in names:
            if self._queues[q]:
                candidates.append((self._queues[q][0], q))
        if not candidates:
            return None
        candidates.sort(key=lambda t: (t[0].priority, t[0].submitted_at, t[0].seq))
        _, best_q = candidates[0]
        entry = heapq.heappop(self._queues[best_q])
        job = self._jobs.get(entry.job_id)
        if job is None or job.status not in (JobStatus.PENDING,):
            return await self._claim_recur_locked(queues)
        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.started_at = now
        job.visibility_deadline = now + job.spec.timeout_s + self.default_visibility_s
        self._running.add(job.id)
        return job

    async def ack(self, job_id: str, result: Any) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.RUNNING:
                return
            job.status = JobStatus.SUCCESS
            job.result = result
            job.finished_at = self.now()
            job.visibility_deadline = None
            self._running.discard(job_id)
            self._total_completed += 1
            # Release dependents.
            for child in self._dependents_of.pop(job_id, set()):
                waits = self._waiting_on.get(child)
                if not waits:
                    continue
                waits.discard(job_id)
                if not waits:
                    self._waiting_on.pop(child, None)
                    cj = self._jobs.get(child)
                    if cj is not None and cj.status == JobStatus.PENDING:
                        self._enqueue(cj)

    async def nack(self, job_id: str, error: str) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.RUNNING:
                return
            await self._nack_locked(job, error)

    async def _nack_locked(self, job: Job, error: str, immediate_requeue: bool = False) -> None:
        job.error = error
        self._running.discard(job.id)
        self._total_failed += 1
        policy = job.spec.retry_policy or self.default_retry
        max_attempts = job.spec.max_attempts or policy.max_attempts
        if job.attempts >= max_attempts:
            job.status = JobStatus.DEAD
            job.finished_at = self.now()
            self._dead.append(job.id)
            self._total_dead += 1
            # Cancel any dependents (they'll never fire).
            for child in self._dependents_of.pop(job.id, set()):
                cj = self._jobs.get(child)
                if cj is None:
                    continue
                cj.status = JobStatus.DEAD
                cj.error = f"upstream {job.id} dead"
                self._dead.append(child)
                self._total_dead += 1
                self._waiting_on.pop(child, None)
            return
        # Retry: schedule with backoff (or requeue immediately, e.g. after a
        # visibility-timeout redelivery where the delay already occurred).
        job.visibility_deadline = None
        if immediate_requeue:
            job.status = JobStatus.PENDING
            job.next_run_at = None
        else:
            backoff = policy.backoff_for_attempt(job.attempts, self._rng.random())
            job.next_run_at = self.now() + backoff
            job.status = JobStatus.SCHEDULED
        self._enqueue(job)

    async def cancel(self, job_id: str) -> bool:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.status in (JobStatus.SUCCESS, JobStatus.DEAD, JobStatus.CANCELLED):
                return False
            job.status = JobStatus.CANCELLED
            self._running.discard(job_id)
            return True

    async def get(self, job_id: str) -> Job | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def dead_letters(self) -> list[Job]:
        async with self._lock:
            return [self._jobs[j] for j in self._dead if j in self._jobs]

    async def stats(self) -> StatsSnapshot:
        async with self._lock:
            return StatsSnapshot(
                total_submitted=self._total_submitted,
                total_completed=self._total_completed,
                total_failed=self._total_failed,
                total_dead=self._total_dead,
                per_queue_depth={q: len(entries) for q, entries in self._queues.items()},
                scheduled_depth=len(self._scheduled),
                dead_depth=len(self._dead),
                running=len(self._running),
            )
