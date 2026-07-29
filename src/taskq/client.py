"""Client SDK. Sync-friendly wrapper around httpx."""
from __future__ import annotations

import time
from typing import Any

import httpx

from .models import Job, JobSpec, JobStatus, StatsSnapshot


class TaskQueueError(Exception):
    pass


class TaskQueueClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8000", *, timeout_s: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout_s)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "TaskQueueClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def submit(self, spec: JobSpec) -> str:
        r = self._client.post("/jobs", json=spec.model_dump(mode="json"))
        r.raise_for_status()
        return str(r.json()["job_id"])

    def get(self, job_id: str) -> Job:
        r = self._client.get(f"/jobs/{job_id}")
        if r.status_code == 404:
            raise TaskQueueError(f"unknown job {job_id}")
        r.raise_for_status()
        return Job.model_validate(r.json())

    def cancel(self, job_id: str) -> bool:
        r = self._client.post(f"/jobs/{job_id}/cancel")
        r.raise_for_status()
        return bool(r.json().get("cancelled"))

    def stats(self) -> StatsSnapshot:
        r = self._client.get("/stats")
        r.raise_for_status()
        return StatsSnapshot.model_validate(r.json())

    def dead(self) -> list[Job]:
        r = self._client.get("/dead")
        r.raise_for_status()
        return [Job.model_validate(j) for j in r.json()]

    def wait(self, job_id: str, *, timeout_s: float = 60.0, poll_interval_s: float = 0.1) -> Job:
        """Poll until the job is in a terminal state or `timeout_s` elapses.

        Raises TaskQueueError on unknown job. Returns the terminal Job either
        way (SUCCESS, DEAD, or CANCELLED); caller inspects .status.
        """
        deadline = time.monotonic() + timeout_s
        terminal = {JobStatus.SUCCESS, JobStatus.DEAD, JobStatus.CANCELLED}
        while True:
            j = self.get(job_id)
            if j.status in terminal:
                return j
            if time.monotonic() >= deadline:
                raise TaskQueueError(f"wait timeout on job {job_id} (still {j.status})")
            time.sleep(poll_interval_s)
