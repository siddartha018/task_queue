"""Worker: pull jobs from the broker over HTTP, execute registered tasks, ack/nack.

Tasks are registered process-locally via `@register("name")`. Timeouts are
enforced with `concurrent.futures.ThreadPoolExecutor` + a hard `Future.result(timeout=)`
watchdog on the calling thread; if the task exceeds `spec.timeout_s`, we nack
with a timeout error. (True hard-kill of a runaway thread is impossible in
CPython, but the broker's visibility timeout is the second line of defence.)
"""
from __future__ import annotations

import concurrent.futures
import logging
import time
from typing import Any, Callable

import httpx

from .models import Job

log = logging.getLogger("taskq.worker")

_REGISTRY: dict[str, Callable[..., Any]] = {}


def register(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator: register a callable as `name` so the worker can execute it."""
    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        _REGISTRY[name] = fn
        return fn
    return wrap


def registered() -> dict[str, Callable[..., Any]]:
    return dict(_REGISTRY)


class Worker:
    def __init__(
        self,
        base_url: str,
        *,
        queues: list[str] | None = None,
        poll_interval_s: float = 0.05,
        max_concurrent: int = 1,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.queues = queues
        self.poll_interval_s = poll_interval_s
        self.max_concurrent = max_concurrent
        self._client = httpx.Client(base_url=self.base_url, timeout=30.0)
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent)
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    def _claim(self) -> Job | None:
        r = self._client.post("/jobs/claim", json={"queues": self.queues})
        if r.status_code == 204:
            return None
        r.raise_for_status()
        return Job.model_validate(r.json())

    def _ack(self, job_id: str, result: Any) -> None:
        r = self._client.post(f"/jobs/{job_id}/ack", json={"result": result})
        r.raise_for_status()

    def _nack(self, job_id: str, error: str) -> None:
        r = self._client.post(f"/jobs/{job_id}/nack", json={"error": error})
        r.raise_for_status()

    def _execute(self, job: Job) -> None:
        fn = _REGISTRY.get(job.spec.task)
        if fn is None:
            self._nack(job.id, f"unknown task '{job.spec.task}'")
            return
        try:
            fut = self._pool.submit(fn, **job.spec.args)
            result = fut.result(timeout=job.spec.timeout_s)
            self._ack(job.id, result)
        except concurrent.futures.TimeoutError:
            self._nack(job.id, f"timeout after {job.spec.timeout_s:.1f}s")
        except Exception as e:
            self._nack(job.id, f"{type(e).__name__}: {e}")

    def run_once(self) -> bool:
        """Try to claim + execute one job. Returns True if a job ran, False if idle."""
        job = self._claim()
        if job is None:
            return False
        self._execute(job)
        return True

    def run_forever(self) -> None:
        log.info("worker starting, queues=%s", self.queues)
        while not self._stopping:
            try:
                did = self.run_once()
            except httpx.HTTPError as e:
                log.warning("http error, retrying: %s", e)
                time.sleep(1.0)
                continue
            except Exception as e:
                log.exception("unexpected worker error: %s", e)
                time.sleep(1.0)
                continue
            if not did:
                time.sleep(self.poll_interval_s)
        log.info("worker stopped")
