"""Wire types shared by broker, client, worker.

Kept as Pydantic v2 models so FastAPI serializes them, and so mypy strict
mode is happy with the field types.
"""
from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(StrEnum):
    PENDING = "pending"     # in-queue, waiting for a worker to claim
    SCHEDULED = "scheduled" # scheduled for a future run_at
    RUNNING = "running"     # claimed by a worker; visibility timeout is armed
    SUCCESS = "success"
    FAILED = "failed"       # retryable failure, will be re-queued
    DEAD = "dead"           # retries exhausted or non-retryable
    CANCELLED = "cancelled"


class RetryPolicy(BaseModel):
    max_attempts: int = 3
    initial_backoff_s: float = 1.0
    max_backoff_s: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_s: float = 0.1

    def backoff_for_attempt(self, attempt: int, rand: float) -> float:
        """attempt is 1-indexed (attempt=1 is the retry AFTER the first failure)."""
        raw = self.initial_backoff_s * (self.backoff_multiplier ** (attempt - 1))
        capped = min(raw, self.max_backoff_s)
        return capped + rand * self.jitter_s


class JobSpec(BaseModel):
    """A submittable unit of work."""
    task: str                      # registered task name; the worker resolves it to a callable
    args: dict[str, Any] = Field(default_factory=dict)
    queue: str = "default"
    priority: int = 5              # 0 = highest, 9 = lowest
    idempotency_key: str | None = None
    max_attempts: int | None = None
    timeout_s: float = 300.0
    run_at: float | None = None    # unix epoch seconds; None = ASAP
    retry_policy: RetryPolicy | None = None
    depends_on: list[str] = Field(default_factory=list)


class Job(BaseModel):
    """A job's server-side record. Includes runtime state."""
    id: str
    spec: JobSpec
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    result: Any = None
    error: str | None = None
    submitted_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    next_run_at: float | None = None       # for scheduled/retrying jobs
    visibility_deadline: float | None = None  # while RUNNING, must ack before this


class StatsSnapshot(BaseModel):
    total_submitted: int
    total_completed: int
    total_failed: int
    total_dead: int
    per_queue_depth: dict[str, int]
    scheduled_depth: int
    dead_depth: int
    running: int
