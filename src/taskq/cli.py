"""taskq CLI: serve broker, run worker, submit + inspect jobs."""
from __future__ import annotations

import importlib
import json
import signal
import sys
from typing import Any, Optional

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from .client import TaskQueueClient
from .models import JobSpec
from .server import build_app
from .worker import Worker, registered

app = typer.Typer(help="taskq: distributed task queue CLI", no_args_is_help=True)
console = Console()


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
) -> None:
    """Run the broker HTTP server."""
    uvicorn.run(build_app(), host=host, port=port, log_level=log_level)


@app.command()
def worker(
    base_url: str = typer.Option("http://127.0.0.1:8000", "--url"),
    queue: list[str] = typer.Option([], "--queue", "-q", help="Restrict to these queues (repeatable)"),
    tasks_module: Optional[str] = typer.Option(
        None, "--tasks", help="Python module path to import; its @register decorators run on import"
    ),
    poll_ms: int = typer.Option(50, "--poll-ms"),
    concurrency: int = typer.Option(1, "--concurrency", "-c"),
) -> None:
    """Run a worker process."""
    if tasks_module:
        importlib.import_module(tasks_module)
    console.print(f"[green]registered tasks:[/green] {list(registered().keys())}")
    w = Worker(
        base_url,
        queues=queue or None,
        poll_interval_s=poll_ms / 1000.0,
        max_concurrent=concurrency,
    )
    signal.signal(signal.SIGINT, lambda *_: w.stop())
    signal.signal(signal.SIGTERM, lambda *_: w.stop())
    w.run_forever()


@app.command()
def submit(
    task: str,
    args: str = typer.Option("{}", help="JSON dict of kwargs"),
    queue: str = "default",
    priority: int = 5,
    max_attempts: Optional[int] = None,
    timeout_s: float = 300.0,
    idempotency_key: Optional[str] = None,
    delay_s: Optional[float] = None,
    depends_on: list[str] = typer.Option([], "--depends-on"),
    base_url: str = typer.Option("http://127.0.0.1:8000", "--url"),
) -> None:
    """Submit a single job."""
    import time as _t
    parsed_args = json.loads(args)
    spec = JobSpec(
        task=task,
        args=parsed_args,
        queue=queue,
        priority=priority,
        max_attempts=max_attempts,
        timeout_s=timeout_s,
        idempotency_key=idempotency_key,
        run_at=(_t.time() + delay_s) if delay_s else None,
        depends_on=depends_on,
    )
    with TaskQueueClient(base_url) as c:
        job_id = c.submit(spec)
        console.print({"job_id": job_id})


@app.command()
def show(job_id: str, base_url: str = typer.Option("http://127.0.0.1:8000", "--url")) -> None:
    """Show a job's current state."""
    with TaskQueueClient(base_url) as c:
        console.print_json(json.dumps(c.get(job_id).model_dump(mode="json"), default=str))


@app.command()
def wait(
    job_id: str,
    timeout_s: float = 60.0,
    base_url: str = typer.Option("http://127.0.0.1:8000", "--url"),
) -> None:
    """Wait for a job to reach a terminal state."""
    with TaskQueueClient(base_url) as c:
        j = c.wait(job_id, timeout_s=timeout_s)
        console.print_json(json.dumps(j.model_dump(mode="json"), default=str))


@app.command()
def stats(base_url: str = typer.Option("http://127.0.0.1:8000", "--url")) -> None:
    """Print broker stats."""
    with TaskQueueClient(base_url) as c:
        s = c.stats()
    t = Table("metric", "value")
    t.add_row("total_submitted", str(s.total_submitted))
    t.add_row("total_completed", str(s.total_completed))
    t.add_row("total_failed", str(s.total_failed))
    t.add_row("total_dead", str(s.total_dead))
    t.add_row("running", str(s.running))
    t.add_row("scheduled_depth", str(s.scheduled_depth))
    t.add_row("dead_depth", str(s.dead_depth))
    for q, d in s.per_queue_depth.items():
        t.add_row(f"queue[{q}]", str(d))
    console.print(t)


@app.command()
def dead(base_url: str = typer.Option("http://127.0.0.1:8000", "--url")) -> None:
    """List dead-letter jobs."""
    with TaskQueueClient(base_url) as c:
        for j in c.dead():
            console.print(f"[red]{j.id}[/red] {j.spec.task} ({j.attempts} attempts) — {j.error}")


if __name__ == "__main__":
    app()
