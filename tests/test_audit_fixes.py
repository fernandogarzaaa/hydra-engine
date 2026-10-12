"""Tests for the Oct 2026 audit fixes: delayed-retry promotion, lease
release/heartbeat, early-retry parking, dead-letter handling, resume retry
budget and optional API-key auth."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import fakeredis
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base, get_db
from app.main import app
from app.models import (
    ExecutionStep,
    StepStatus,
    StepType,
    WorkflowStatus,
    WorkflowTrajectory,
    utc_now,
)
from app.worker import (
    _renew_lease,
    enqueue_trajectory,
    handle_queue_item,
    process_trajectory,
    promote_due_retries,
)

QUEUE = "hydra:workflow:queue"
DELAYED = "hydra:workflow:delayed"
DEAD = "hydra:workflow:dead"


@pytest.fixture()
async def session_factory() -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.fixture()
async def redis() -> AsyncGenerator[fakeredis.FakeAsyncRedis, None]:
    client = fakeredis.FakeAsyncRedis()
    yield client
    await client.aclose()


async def _make_trajectory(
    factory: async_sessionmaker[AsyncSession],
    *,
    steps: int = 1,
    status: str = WorkflowStatus.PENDING.value,
    step_status: str = StepStatus.PENDING.value,
    retry_count: int = 0,
    claimed_by: str | None = None,
    lease_expires_at: Any = None,
) -> uuid.UUID:
    tid = uuid.uuid4()
    async with factory() as session:
        session.add(
            WorkflowTrajectory(
                id=tid,
                tenant_id="t",
                status=status,
                claimed_by=claimed_by,
                lease_expires_at=lease_expires_at,
            )
        )
        for n in range(1, steps + 1):
            session.add(
                ExecutionStep(
                    trajectory_id=tid,
                    step_number=n,
                    step_type=StepType.LLM_THOUGHT.value,
                    input_payload={"n": n},
                    status=step_status,
                    retry_count=retry_count,
                )
            )
        await session.commit()
    return tid


async def _load(
    factory: async_sessionmaker[AsyncSession], tid: uuid.UUID
) -> tuple[WorkflowTrajectory, list[ExecutionStep]]:
    async with factory() as session:
        traj = (
            await session.execute(select(WorkflowTrajectory).where(WorkflowTrajectory.id == tid))
        ).scalar_one()
        steps = list(
            (
                await session.execute(
                    select(ExecutionStep)
                    .where(ExecutionStep.trajectory_id == tid)
                    .order_by(ExecutionStep.step_number)
                )
            )
            .scalars()
            .all()
        )
    return traj, steps


# --- delayed-retry promotion -------------------------------------------------


@pytest.mark.asyncio()
async def test_promote_moves_only_due_entries(redis: fakeredis.FakeAsyncRedis) -> None:
    now = 1_000.0
    await redis.zadd(DELAYED, {"due-a": now - 5, "due-b": now, "later": now + 60})

    promoted = await promote_due_retries(redis, now=now)

    assert promoted == 2
    assert sorted(await redis.lrange(QUEUE, 0, -1)) == [b"due-a", b"due-b"]
    assert await redis.zrange(DELAYED, 0, -1) == [b"later"]
    # Nothing left that is due: a second pass promotes nothing.
    assert await promote_due_retries(redis, now=now) == 0
    assert await redis.llen(QUEUE) == 2


@pytest.mark.asyncio()
async def test_promote_respects_batch_size(redis: fakeredis.FakeAsyncRedis) -> None:
    await redis.zadd(DELAYED, {f"m{i}": float(i) for i in range(5)})
    assert await promote_due_retries(redis, now=100.0, batch_size=2) == 2
    assert await redis.llen(QUEUE) == 2
    assert await redis.zcard(DELAYED) == 3


@pytest.mark.asyncio()
async def test_promote_skips_entry_another_worker_already_removed() -> None:
    client = AsyncMock()
    client.zrangebyscore.return_value = [b"x", b"y"]
    client.zrem.side_effect = [0, 1]  # "x" was taken by another worker

    assert await promote_due_retries(client, now=1.0) == 1
    client.lpush.assert_awaited_once_with(QUEUE, b"y")


@pytest.mark.asyncio()
async def test_failed_step_retry_round_trips_through_delayed_set(
    session_factory: async_sessionmaker[AsyncSession],
    redis: fakeredis.FakeAsyncRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.worker.settings.MAX_STEP_RETRIES", 3)
    monkeypatch.setattr("app.worker.settings.BACKOFF_FACTOR", 2.0)
    tid = await _make_trajectory(session_factory)
    calls = {"n": 0}

    async def flaky(step: ExecutionStep, _replay: list[dict[str, Any]]) -> dict[str, Any]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return {"ok": True}

    await process_trajectory(
        str(tid), session_factory=session_factory, redis_client=redis, tool_executor=flaky
    )
    # Retry is parked, lease released, nothing on the work queue yet.
    traj, steps = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.PENDING.value
    assert traj.claimed_by is None and traj.lease_expires_at is None
    assert steps[0].retry_count == 1
    assert await redis.zcard(DELAYED) == 1
    assert await redis.llen(QUEUE) == 0
    (_, score), *_ = await redis.zrange(DELAYED, 0, -1, withscores=True)
    assert score > time.time()  # scheduled in the future (2**1 seconds)

    # Not due yet -> not promoted; once due -> promoted and processed.
    assert await promote_due_retries(redis, now=time.time()) == 0
    assert await promote_due_retries(redis, now=score + 0.001) == 1
    item = await redis.brpop(QUEUE, 1)
    assert item is not None
    assert await handle_queue_item(
        item, session_factory=session_factory, redis_client=redis, tool_executor=flaky
    )

    traj, steps = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.COMPLETED.value
    assert steps[0].status == StepStatus.COMPLETED.value
    assert steps[0].output_payload == {"ok": True}
    assert traj.claimed_by is None and traj.lease_expires_at is None
    assert calls["n"] == 2


# --- lease release / heartbeat ----------------------------------------------


@pytest.mark.asyncio()
async def test_lease_released_on_completion(
    session_factory: async_sessionmaker[AsyncSession], redis: fakeredis.FakeAsyncRedis
) -> None:
    tid = await _make_trajectory(session_factory, steps=2)

    async def ok(step: ExecutionStep, _r: list[dict[str, Any]]) -> dict[str, Any]:
        return {"n": step.step_number}

    await process_trajectory(
        str(tid), session_factory=session_factory, redis_client=redis, tool_executor=ok
    )
    traj, steps = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.COMPLETED.value
    assert traj.claimed_by is None
    assert traj.lease_expires_at is None
    assert [s.status for s in steps] == [StepStatus.COMPLETED.value] * 2


@pytest.mark.asyncio()
async def test_lease_released_on_permanent_failure(
    session_factory: async_sessionmaker[AsyncSession],
    redis: fakeredis.FakeAsyncRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.worker.settings.MAX_STEP_RETRIES", 1)
    tid = await _make_trajectory(session_factory)

    async def boom(step: ExecutionStep, _r: list[dict[str, Any]]) -> dict[str, Any]:
        raise RuntimeError("nope")

    await process_trajectory(
        str(tid), session_factory=session_factory, redis_client=redis, tool_executor=boom
    )
    traj, steps = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.FAILED.value
    assert steps[0].status == StepStatus.FAILED.value
    assert steps[0].retry_count == 1
    assert traj.claimed_by is None and traj.lease_expires_at is None
    assert await redis.zcard(DELAYED) == 0  # no retry scheduled
    assert await redis.llen(QUEUE) == 0


@pytest.mark.asyncio()
async def test_exhausted_step_fails_without_running_and_releases_lease(
    session_factory: async_sessionmaker[AsyncSession],
    redis: fakeredis.FakeAsyncRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.worker.settings.MAX_STEP_RETRIES", 2)
    tid = await _make_trajectory(
        session_factory, step_status=StepStatus.FAILED.value, retry_count=2
    )
    executor = AsyncMock()

    await process_trajectory(
        str(tid), session_factory=session_factory, redis_client=redis, tool_executor=executor
    )
    executor.assert_not_awaited()
    traj, _ = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.FAILED.value
    assert traj.claimed_by is None and traj.lease_expires_at is None


@pytest.mark.asyncio()
async def test_heartbeat_renews_lease_during_long_step(
    session_factory: async_sessionmaker[AsyncSession],
    redis: fakeredis.FakeAsyncRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.worker.settings.TRAJECTORY_LEASE_SECONDS", 0.3)
    tid = await _make_trajectory(session_factory)
    seen: list[Any] = []

    async def slow(step: ExecutionStep, _r: list[dict[str, Any]]) -> dict[str, Any]:
        for _ in range(4):
            await asyncio.sleep(0.15)
            traj, _steps = await _load(session_factory, tid)
            seen.append(traj.lease_expires_at)
        return {"done": True}

    await process_trajectory(
        str(tid),
        session_factory=session_factory,
        redis_client=redis,
        tool_executor=slow,
        worker_id="w1",
    )
    # The lease moved forward while the 0.6 s step ran (lease is only 0.3 s).
    assert len({v for v in seen if v is not None}) >= 2
    assert seen[-1] > seen[0]
    traj, _ = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.COMPLETED.value
    assert traj.lease_expires_at is None


@pytest.mark.asyncio()
async def test_renew_lease_only_for_current_holder(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    old = utc_now() + timedelta(seconds=5)
    tid = await _make_trajectory(session_factory, claimed_by="holder", lease_expires_at=old)

    assert await _renew_lease(session_factory, tid, "intruder", 60) is False
    traj, _ = await _load(session_factory, tid)
    assert traj.lease_expires_at.replace(tzinfo=None) == old.replace(tzinfo=None)

    assert await _renew_lease(session_factory, tid, "holder", 60) is True
    traj, _ = await _load(session_factory, tid)
    assert traj.lease_expires_at.replace(tzinfo=None) > old.replace(tzinfo=None)


# --- early retry parking -----------------------------------------------------


@pytest.mark.asyncio()
async def test_message_arriving_while_leased_elsewhere_is_parked_not_dropped(
    session_factory: async_sessionmaker[AsyncSession], redis: fakeredis.FakeAsyncRedis
) -> None:
    lease_end = utc_now() + timedelta(seconds=30)
    tid = await _make_trajectory(
        session_factory,
        status=WorkflowStatus.RUNNING.value,
        claimed_by="other-worker",
        lease_expires_at=lease_end,
    )
    executor = AsyncMock()
    before = time.time()

    await process_trajectory(
        str(tid), session_factory=session_factory, redis_client=redis, tool_executor=executor
    )

    executor.assert_not_awaited()
    entries = await redis.zrange(DELAYED, 0, -1, withscores=True)
    assert len(entries) == 1
    payload, score = entries[0]
    assert json.loads(payload) == {"trajectory_id": str(tid)}
    # Parked until roughly when the other worker's lease ends.
    assert before + 25 <= score <= before + 35
    traj, _ = await _load(session_factory, tid)
    assert traj.claimed_by == "other-worker"


@pytest.mark.asyncio()
async def test_message_for_completed_trajectory_is_not_parked(
    session_factory: async_sessionmaker[AsyncSession], redis: fakeredis.FakeAsyncRedis
) -> None:
    tid = await _make_trajectory(
        session_factory,
        status=WorkflowStatus.COMPLETED.value,
        step_status=StepStatus.COMPLETED.value,
    )
    executor = AsyncMock()
    await process_trajectory(
        str(tid), session_factory=session_factory, redis_client=redis, tool_executor=executor
    )
    executor.assert_not_awaited()
    assert await redis.zcard(DELAYED) == 0
    assert await redis.llen(QUEUE) == 0


@pytest.mark.asyncio()
async def test_enqueue_immediate_vs_delayed(redis: fakeredis.FakeAsyncRedis) -> None:
    tid = uuid.uuid4()
    await enqueue_trajectory(redis, tid)
    assert await redis.lrange(QUEUE, 0, -1) == [json.dumps({"trajectory_id": str(tid)}).encode()]
    assert await redis.zcard(DELAYED) == 0
    await enqueue_trajectory(redis, tid, delay_seconds=10)
    assert await redis.llen(QUEUE) == 1
    assert await redis.zcard(DELAYED) == 1


# --- dead letter / loop survival --------------------------------------------


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "item",
    [
        (b"q", b"not json"),
        (b"q", b'{"other": 1}'),
        (b"q", b'{"trajectory_id": "not-a-uuid"}'),
        (b"q", b"\xff\xfe"),
        (b"only-one",),
    ],
)
async def test_malformed_message_goes_to_dead_letter(
    session_factory: async_sessionmaker[AsyncSession],
    redis: fakeredis.FakeAsyncRedis,
    item: Any,
) -> None:
    executor = AsyncMock()
    ok = await handle_queue_item(
        item, session_factory=session_factory, redis_client=redis, tool_executor=executor
    )
    assert ok is False
    executor.assert_not_awaited()
    dead = await redis.lrange(DEAD, 0, -1)
    assert dead == [repr(item).encode()]


@pytest.mark.asyncio()
async def test_processing_crash_is_contained(
    redis: fakeredis.FakeAsyncRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    crash = AsyncMock(side_effect=RuntimeError("db down"))
    monkeypatch.setattr("app.worker.process_trajectory", crash)
    item = (b"q", json.dumps({"trajectory_id": str(uuid.uuid4())}).encode())

    ok = await handle_queue_item(item, redis_client=redis)

    assert ok is False
    crash.assert_awaited_once()
    assert await redis.llen(DEAD) == 0  # well-formed: not dead-lettered


@pytest.mark.asyncio()
async def test_valid_message_is_processed(
    session_factory: async_sessionmaker[AsyncSession], redis: fakeredis.FakeAsyncRedis
) -> None:
    tid = await _make_trajectory(session_factory)

    async def ok(step: ExecutionStep, _r: list[dict[str, Any]]) -> dict[str, Any]:
        return {}

    item = (b"q", json.dumps({"trajectory_id": str(tid)}))
    assert await handle_queue_item(
        item, session_factory=session_factory, redis_client=redis, tool_executor=ok
    )
    traj, _ = await _load(session_factory, tid)
    assert traj.status == WorkflowStatus.COMPLETED.value


# --- API: resume budget + HYDRA_API_KEY ------------------------------------


@pytest.fixture()
async def api(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[AsyncClient, None]:
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr("app.main.make_redis_client", lambda: AsyncMock())
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio()
async def test_resume_resets_retry_budget(
    api: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.main.settings.HYDRA_API_KEY", "")
    tid = await _make_trajectory(
        session_factory,
        status=WorkflowStatus.FAILED.value,
        step_status=StepStatus.FAILED.value,
        retry_count=3,
    )
    queue = AsyncMock()
    monkeypatch.setattr("app.main.make_redis_client", lambda: queue)
    r = await api.post(f"/workflows/{tid}/resume")
    assert r.status_code == 202
    queue.lpush.assert_awaited_once_with(QUEUE, json.dumps({"trajectory_id": str(tid)}))
    queue.aclose.assert_awaited_once()
    _, steps = await _load(session_factory, tid)
    assert steps[0].status == StepStatus.PENDING.value
    assert steps[0].retry_count == 0


@pytest.mark.asyncio()
async def test_api_open_when_no_key_configured(
    api: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.main.settings.HYDRA_API_KEY", "")
    tid = await _make_trajectory(session_factory)
    assert (await api.get(f"/workflows/{tid}/audit")).status_code == 200


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/workflows/trigger"),
        ("post", "/workflows/{tid}/resume"),
        ("get", "/workflows/{tid}/audit"),
    ],
)
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-API-Key": "wrong"},
        {"Authorization": "Bearer wrong"},
        {"Authorization": "s3cret"},  # no Bearer scheme
        {"X-API-Key": ""},
    ],
)
async def test_api_rejects_missing_or_wrong_key(
    api: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    headers: dict[str, str],
) -> None:
    monkeypatch.setattr("app.main.settings.HYDRA_API_KEY", "s3cret")
    tid = await _make_trajectory(session_factory)
    kwargs: dict[str, Any] = {"headers": headers}
    if method == "post" and path.endswith("trigger"):
        kwargs["json"] = {"tenant_id": "t"}
    r = await getattr(api, method)(path.format(tid=tid), **kwargs)
    assert r.status_code == 401
    assert r.headers.get("www-authenticate") == "Bearer"


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "headers",
    [
        {"X-API-Key": "s3cret"},
        {"Authorization": "Bearer s3cret"},
        {"authorization": "bearer s3cret"},
    ],
)
async def test_api_accepts_correct_key(
    api: AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
) -> None:
    monkeypatch.setattr("app.main.settings.HYDRA_API_KEY", "s3cret")
    tid = await _make_trajectory(session_factory)
    r = await api.get(f"/workflows/{tid}/audit", headers=headers)
    assert r.status_code == 200
    assert str(tid) in r.text


@pytest.mark.asyncio()
async def test_healthz_does_not_require_key(
    api: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.main.settings.HYDRA_API_KEY", "s3cret")
    assert (await api.get("/healthz")).status_code == 200
