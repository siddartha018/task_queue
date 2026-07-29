"""End-to-end: spin up FastAPI in a thread, run a real worker against it."""
from __future__ import annotations

import threading
import time

import pytest
import uvicorn

from taskq import register
from taskq.client import TaskQueueClient
from taskq.models import JobSpec, JobStatus
from taskq.server import build_app
from taskq.worker import Worker


class _ThreadedServer:
    def __init__(self, app, host: str = "127.0.0.1", port: int = 0) -> None:  # noqa: ANN001
        self.config = uvicorn.Config(app, host=host, port=port, log_level="warning")
        self.server = uvicorn.Server(self.config)
        self._thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self._thread.start()
        for _ in range(200):
            if self.server.started:
                break
            time.sleep(0.02)

    @property
    def url(self) -> str:
        # uvicorn opens a listening socket in self.server.servers[0].sockets
        sock = self.server.servers[0].sockets[0]
        addr = sock.getsockname()
        return f"http://{addr[0]}:{addr[1]}"

    def stop(self) -> None:
        self.server.should_exit = True
        self._thread.join(timeout=5)


@register("add")
def _add(a: int, b: int) -> int:
    return a + b


@register("boom")
def _boom() -> None:
    raise RuntimeError("kaboom")


@pytest.fixture
def server():  # noqa: ANN201
    app = build_app()
    srv = _ThreadedServer(app)
    srv.start()
    yield srv
    srv.stop()


def test_end_to_end_success(server) -> None:  # noqa: ANN001
    c = TaskQueueClient(server.url)
    w = Worker(server.url, poll_interval_s=0.01)

    job_id = c.submit(JobSpec(task="add", args={"a": 2, "b": 3}))
    # One worker turn should be enough.
    for _ in range(50):
        if w.run_once():
            break
        time.sleep(0.02)
    j = c.wait(job_id, timeout_s=5.0)
    assert j.status == JobStatus.SUCCESS
    assert j.result == 5


def test_end_to_end_retry_then_dead(server) -> None:  # noqa: ANN001
    c = TaskQueueClient(server.url)
    w = Worker(server.url, poll_interval_s=0.01)

    job_id = c.submit(JobSpec(task="boom", max_attempts=2, timeout_s=5.0))

    # Attempt 1: nacked, goes to scheduled with a 1s backoff.
    for _ in range(200):
        if w.run_once():
            break
        time.sleep(0.02)

    # Wait for the retry to become due AND the second attempt to run.
    end = time.time() + 5.0
    while time.time() < end:
        w.run_once()
        j = c.get(job_id)
        if j.status in (JobStatus.DEAD, JobStatus.SUCCESS):
            break
        time.sleep(0.05)
    j = c.get(job_id)
    assert j.status == JobStatus.DEAD
    assert j.attempts == 2


def test_end_to_end_unknown_task(server) -> None:  # noqa: ANN001
    c = TaskQueueClient(server.url)
    w = Worker(server.url, poll_interval_s=0.01)
    job_id = c.submit(JobSpec(task="does_not_exist", max_attempts=1))
    for _ in range(50):
        if w.run_once():
            break
        time.sleep(0.02)
    j = c.wait(job_id, timeout_s=5.0)
    assert j.status == JobStatus.DEAD
    assert j.error and "unknown task" in j.error
