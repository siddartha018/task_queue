from __future__ import annotations

import pytest

from taskq.broker import Broker
from taskq.models import JobSpec, JobStatus


@pytest.mark.asyncio
async def test_submit_and_claim(broker: Broker) -> None:
    jid = await broker.submit(JobSpec(task="add", args={"a": 1, "b": 2}))
    j = await broker.get(jid)
    assert j is not None
    assert j.status == JobStatus.PENDING

    claimed = await broker.claim()
    assert claimed is not None
    assert claimed.id == jid
    assert claimed.status == JobStatus.RUNNING
    assert claimed.attempts == 1


@pytest.mark.asyncio
async def test_ack_marks_success(broker: Broker) -> None:
    jid = await broker.submit(JobSpec(task="add"))
    await broker.claim()
    await broker.ack(jid, {"answer": 42})
    j = await broker.get(jid)
    assert j is not None
    assert j.status == JobStatus.SUCCESS
    assert j.result == {"answer": 42}


@pytest.mark.asyncio
async def test_priority_ordering(broker: Broker, clock) -> None:  # noqa: ANN001
    low = await broker.submit(JobSpec(task="t", priority=9))
    high = await broker.submit(JobSpec(task="t", priority=1))
    mid = await broker.submit(JobSpec(task="t", priority=5))
    j1 = await broker.claim()
    j2 = await broker.claim()
    j3 = await broker.claim()
    assert (j1.id, j2.id, j3.id) == (high, mid, low)


@pytest.mark.asyncio
async def test_fifo_within_same_priority(broker: Broker, clock) -> None:  # noqa: ANN001
    a = await broker.submit(JobSpec(task="t", priority=5))
    clock.advance(0.001)
    b = await broker.submit(JobSpec(task="t", priority=5))
    assert (await broker.claim()).id == a
    assert (await broker.claim()).id == b


@pytest.mark.asyncio
async def test_retry_then_dead(broker: Broker, clock) -> None:  # noqa: ANN001
    jid = await broker.submit(JobSpec(task="t"))
    for i in range(3):
        j = await broker.claim()
        assert j is not None
        await broker.nack(jid, f"attempt {i+1} fail")
        # Job is scheduled with backoff; advance clock past it.
        clock.advance(60.0)
        await broker.promote_due_scheduled()
    final = await broker.get(jid)
    assert final is not None
    assert final.status == JobStatus.DEAD
    assert final.attempts == 3


@pytest.mark.asyncio
async def test_retry_then_success(broker: Broker, clock) -> None:  # noqa: ANN001
    jid = await broker.submit(JobSpec(task="t"))
    await broker.claim()
    await broker.nack(jid, "flaky")
    j = await broker.get(jid)
    assert j is not None and j.status == JobStatus.SCHEDULED
    clock.advance(60.0)
    moved = await broker.promote_due_scheduled()
    assert moved >= 1
    await broker.claim()
    await broker.ack(jid, "ok")
    j = await broker.get(jid)
    assert j is not None and j.status == JobStatus.SUCCESS


@pytest.mark.asyncio
async def test_visibility_timeout_requeues(broker: Broker, clock) -> None:  # noqa: ANN001
    jid = await broker.submit(JobSpec(task="t", timeout_s=1.0))
    claimed = await broker.claim()
    assert claimed is not None
    # Simulate worker death: never ack, let the visibility deadline expire.
    clock.advance(1000.0)  # blow past timeout + default_visibility_s
    moved = await broker.promote_due_scheduled()
    assert moved >= 1
    reclaim = await broker.claim()
    assert reclaim is not None
    assert reclaim.id == jid
    assert reclaim.attempts == 2  # attempts counter incremented on second claim


@pytest.mark.asyncio
async def test_idempotency_returns_same_id(broker: Broker) -> None:
    a = await broker.submit(JobSpec(task="t", idempotency_key="k1"))
    b = await broker.submit(JobSpec(task="t", idempotency_key="k1"))
    assert a == b


@pytest.mark.asyncio
async def test_scheduled_run_at(broker: Broker, clock) -> None:  # noqa: ANN001
    future = clock.time() + 30.0
    jid = await broker.submit(JobSpec(task="t", run_at=future))
    # Not runnable yet.
    assert await broker.claim() is None
    j = await broker.get(jid)
    assert j is not None and j.status == JobStatus.SCHEDULED
    clock.advance(30.0)
    assert await broker.promote_due_scheduled() == 1
    claimed = await broker.claim()
    assert claimed is not None and claimed.id == jid


@pytest.mark.asyncio
async def test_dependencies_gate_execution(broker: Broker) -> None:
    parent = await broker.submit(JobSpec(task="t"))
    child = await broker.submit(JobSpec(task="t", depends_on=[parent]))
    # Child not runnable yet.
    first = await broker.claim()
    assert first is not None and first.id == parent
    # Still no second claimable job.
    assert await broker.claim() is None
    await broker.ack(parent, None)
    # Now child is runnable.
    second = await broker.claim()
    assert second is not None and second.id == child


@pytest.mark.asyncio
async def test_dead_dependency_dies_downstream(broker: Broker, clock) -> None:  # noqa: ANN001
    parent = await broker.submit(JobSpec(task="t"))
    child = await broker.submit(JobSpec(task="t", depends_on=[parent]))
    # Fail parent 3 times -> DEAD.
    for _ in range(3):
        await broker.claim()
        await broker.nack(parent, "fail")
        clock.advance(60.0)
        await broker.promote_due_scheduled()
    cj = await broker.get(child)
    assert cj is not None and cj.status == JobStatus.DEAD


@pytest.mark.asyncio
async def test_stats(broker: Broker) -> None:
    for i in range(4):
        await broker.submit(JobSpec(task="t", queue="q1" if i < 2 else "q2"))
    s = await broker.stats()
    assert s.total_submitted == 4
    assert s.per_queue_depth == {"q1": 2, "q2": 2}
    assert s.scheduled_depth == 0


@pytest.mark.asyncio
async def test_cancel(broker: Broker) -> None:
    jid = await broker.submit(JobSpec(task="t"))
    assert await broker.cancel(jid)
    # Not double-cancelable.
    assert not await broker.cancel(jid)
    j = await broker.get(jid)
    assert j is not None and j.status == JobStatus.CANCELLED
