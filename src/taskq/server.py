"""FastAPI HTTP surface over Broker.

Endpoints (all JSON):
  POST /jobs            {spec}        -> {job_id}
  GET  /jobs/{id}                     -> Job
  POST /jobs/claim      {queues}      -> Job | 204
  POST /jobs/{id}/ack   {result}      -> 200
  POST /jobs/{id}/nack  {error}       -> 200
  POST /jobs/{id}/cancel              -> {cancelled: bool}
  GET  /dead                          -> [Job]
  GET  /stats                         -> StatsSnapshot
  GET  /metrics                       -> Prometheus exposition
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, generate_latest
from pydantic import BaseModel

from .broker import Broker
from .models import Job, JobSpec, StatsSnapshot

log = logging.getLogger("taskq.server")


class ClaimRequest(BaseModel):
    queues: list[str] | None = None


class AckRequest(BaseModel):
    result: Any = None


class NackRequest(BaseModel):
    error: str


def build_app(broker: Broker | None = None) -> FastAPI:
    b = broker or Broker()

    registry = CollectorRegistry()
    submitted = Counter("taskq_submitted_total", "Total jobs submitted", ["queue"], registry=registry)
    completed = Counter("taskq_completed_total", "Total jobs completed", ["queue"], registry=registry)
    failed = Counter("taskq_failed_total", "Total job failures (pre-dead)", ["queue"], registry=registry)
    dead = Counter("taskq_dead_total", "Total jobs sent to DLQ", ["queue"], registry=registry)
    depth = Gauge("taskq_queue_depth", "Queue depth", ["queue"], registry=registry)
    sched_depth = Gauge("taskq_scheduled_depth", "Scheduled (delayed/retrying) depth", registry=registry)
    running = Gauge("taskq_running", "Jobs currently claimed / running", registry=registry)

    async def _promote_loop() -> None:
        while True:
            try:
                await b.promote_due_scheduled()
            except Exception as e:  # pragma: no cover
                log.exception("promote loop error: %s", e)
            await asyncio.sleep(0.1)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):  # type: ignore[no-untyped-def]
        task = asyncio.create_task(_promote_loop())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="taskq", lifespan=lifespan)
    app.state.broker = b  # exposed for tests

    @app.post("/jobs")
    async def submit(spec: JobSpec) -> dict[str, str]:
        job_id = await b.submit(spec)
        submitted.labels(queue=spec.queue).inc()
        return {"job_id": job_id}

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> Job:
        j = await b.get(job_id)
        if j is None:
            raise HTTPException(status_code=404, detail="unknown job")
        return j

    @app.post("/jobs/claim", response_model=None)
    async def claim(req: ClaimRequest) -> Job | Response:
        j = await b.claim(req.queues)
        if j is None:
            return Response(status_code=204)
        return j

    @app.post("/jobs/{job_id}/ack")
    async def ack(job_id: str, req: AckRequest) -> dict[str, bool]:
        job = await b.get(job_id)
        await b.ack(job_id, req.result)
        if job is not None:
            completed.labels(queue=job.spec.queue).inc()
        return {"ok": True}

    @app.post("/jobs/{job_id}/nack")
    async def nack(job_id: str, req: NackRequest) -> dict[str, bool]:
        job = await b.get(job_id)
        await b.nack(job_id, req.error)
        if job is not None:
            failed.labels(queue=job.spec.queue).inc()
            # If it just became dead, count that too.
            fresh = await b.get(job_id)
            if fresh is not None and fresh.status.value == "dead":
                dead.labels(queue=job.spec.queue).inc()
        return {"ok": True}

    @app.post("/jobs/{job_id}/cancel")
    async def cancel(job_id: str) -> dict[str, bool]:
        return {"cancelled": await b.cancel(job_id)}

    @app.get("/dead")
    async def deadletters() -> list[Job]:
        return await b.dead_letters()

    @app.get("/stats")
    async def stats() -> StatsSnapshot:
        s = await b.stats()
        for q, d in s.per_queue_depth.items():
            depth.labels(queue=q).set(d)
        sched_depth.set(s.scheduled_depth)
        running.set(s.running)
        return s

    @app.get("/healthz")
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/metrics")
    async def metrics() -> Response:
        # Snap gauges before scrape.
        s = await b.stats()
        for q, d in s.per_queue_depth.items():
            depth.labels(queue=q).set(d)
        sched_depth.set(s.scheduled_depth)
        running.set(s.running)
        return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app
