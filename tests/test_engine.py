"""Integration validation suite for Hydra Engine."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

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
from app.worker import _claim_trajectory, process_trajectory


@pytest.fixture()
async def session_factory() -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def create_trajectory(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    completed_prefix: int = 0,
) -> uuid.UUID:
    trajectory_id = uuid.uuid4()
    async with session_factory() as session:
        trajectory = WorkflowTrajectory(
            id=trajectory_id,
            tenant_id="tenant-a",
            status=WorkflowStatus.PENDING.value,
        )
        session.add(trajectory)
        for step_number in range(1, 4):
            completed = step_number <= completed_prefix
            session.add(
                ExecutionStep(
                    trajectory_id=trajectory_id,
                    step_number=step_number,
                    step_type=(
                        StepType.TOOL_CALL.value if step_number == 3 else StepType.LLM_THOUGHT.value
                    ),
                    input_payload={"step": step_number},
                    output_payload={"already_done": step_number} if completed else None,
                    status=StepStatus.COMPLETED.value if completed else StepStatus.PENDING.value,
                )
            )
        await session.commit()
    return trajectory_id


@pytest.mark.asyncio()
async def test_state_replay_skips_completed_steps_and_resumes_at_step_three(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    trajectory_id = await create_trajectory(session_factory, completed_prefix=2)
    redis_client = AsyncMock()
    executed_steps: list[int] = []

    async def executor(
        step: ExecutionStep,
        replay_state: list[dict[str, Any]],
    ) -> dict[str, Any]:
        executed_steps.append(step.step_number)
        assert [item["step_number"] for item in replay_state] == [1, 2]
        return {"resumed": step.step_number}

    await process_trajectory(
        str(trajectory_id),
        session_factory=session_factory,
        redis_client=redis_client,
        tool_executor=executor,
    )

    async with session_factory() as session:
        steps_result = await session.execute(
            select(ExecutionStep)
            .where(ExecutionStep.trajectory_id == trajectory_id)
            .order_by(ExecutionStep.step_number.asc())
        )
        steps = list(steps_result.scalars().all())
        trajectory_result = await session.execute(
            select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
        )
        trajectory = trajectory_result.scalar_one()

    assert executed_steps == [3]
    assert [step.status for step in steps] == [
        StepStatus.COMPLETED.value,
        StepStatus.COMPLETED.value,
        StepStatus.COMPLETED.value,
    ]
    assert steps[2].output_payload == {"resumed": 3}
    assert trajectory.status == WorkflowStatus.COMPLETED.value
    redis_client.zadd.assert_not_awaited()


@pytest.mark.asyncio()
async def test_backoff_records_failure_and_requeues_with_delay(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trajectory_id = await create_trajectory(session_factory, completed_prefix=0)
    redis_client = AsyncMock()
    monkeypatch.setattr("app.worker.settings.BACKOFF_FACTOR", 3.0)
    monkeypatch.setattr("app.worker.settings.MAX_STEP_RETRIES", 3)

    async def failing_executor(
        step: ExecutionStep,
        replay_state: list[dict[str, Any]],
    ) -> dict[str, Any]:
        assert replay_state == []
        raise RuntimeError(f"boom at step {step.step_number}")

    await process_trajectory(
        str(trajectory_id),
        session_factory=session_factory,
        redis_client=redis_client,
        tool_executor=failing_executor,
    )

    async with session_factory() as session:
        steps_result = await session.execute(
            select(ExecutionStep)
            .where(ExecutionStep.trajectory_id == trajectory_id)
            .order_by(ExecutionStep.step_number.asc())
        )
        steps = list(steps_result.scalars().all())
        trajectory_result = await session.execute(
            select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
        )
        trajectory = trajectory_result.scalar_one()

    failed_step = steps[0]
    assert failed_step.status == StepStatus.FAILED.value
    assert failed_step.retry_count == 1
    assert failed_step.error_log is not None
    assert "RuntimeError: boom at step 1" in failed_step.error_log
    assert trajectory.status == WorkflowStatus.PENDING.value

    redis_client.zadd.assert_awaited_once()
    queue_name = redis_client.zadd.await_args.args[0]
    mapping = redis_client.zadd.await_args.args[1]
    assert queue_name == "hydra:workflow:delayed"
    assert len(mapping) == 1
    run_at = next(iter(mapping.values()))
    assert isinstance(run_at, float)
    assert run_at > 0.0
    redis_client.lpush.assert_not_awaited()


@pytest.mark.asyncio()
async def test_claim_trajectory_only_one_worker_wins(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The atomic claim UPDATE only ever succeeds for a single caller.

    This directly proves the fix for the concurrency gap: two workers racing
    to claim the same pending trajectory cannot both win, because the second
    ``UPDATE ... WHERE`` observes the row already claimed with a live lease
    and therefore affects zero rows.
    """

    trajectory_id = await create_trajectory(session_factory, completed_prefix=0)

    async with session_factory() as session_a, session_factory() as session_b:
        worker_a_claimed = await _claim_trajectory(
            session_a, trajectory_id, worker_id="worker-a", lease_seconds=120.0
        )
        worker_b_claimed = await _claim_trajectory(
            session_b, trajectory_id, worker_id="worker-b", lease_seconds=120.0
        )

    assert worker_a_claimed is True
    assert worker_b_claimed is False

    async with session_factory() as session:
        result = await session.execute(
            select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
        )
        trajectory = result.scalar_one()

    assert trajectory.claimed_by == "worker-a"
    assert trajectory.status == WorkflowStatus.RUNNING.value


@pytest.mark.asyncio()
async def test_claim_trajectory_reclaims_after_lease_expiry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A trajectory whose claiming worker crashed (lease expired) is reclaimable."""

    trajectory_id = await create_trajectory(session_factory, completed_prefix=0)

    async with session_factory() as session:
        claimed = await _claim_trajectory(
            session, trajectory_id, worker_id="worker-a", lease_seconds=120.0
        )
        assert claimed is True

        # Simulate worker-a's lease already having expired (e.g. it crashed).
        result = await session.execute(
            select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
        )
        trajectory = result.scalar_one()
        trajectory.lease_expires_at = utc_now() - timedelta(seconds=1)
        await session.commit()

    async with session_factory() as session:
        reclaimed = await _claim_trajectory(
            session, trajectory_id, worker_id="worker-b", lease_seconds=120.0
        )

    assert reclaimed is True

    async with session_factory() as session:
        result = await session.execute(
            select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
        )
        trajectory = result.scalar_one()

    assert trajectory.claimed_by == "worker-b"


@pytest.mark.asyncio()
async def test_concurrent_process_trajectory_pulls_execute_each_step_exactly_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Two workers polling the same pending trajectory concurrently must not
    double-execute its steps.

    Without the atomic claim, both ``process_trajectory`` calls would load the
    same pending steps and both would run the executor for them. With the
    claim in place, only the winning call proceeds past the claim and the
    loser returns immediately, so every step's executor runs exactly once.
    """

    trajectory_id = await create_trajectory(session_factory, completed_prefix=0)
    executions: list[int] = []
    execution_lock = asyncio.Lock()

    async def executor(
        step: ExecutionStep,
        replay_state: list[dict[str, Any]],
    ) -> dict[str, Any]:
        # Yield control to widen the race window between the two concurrent
        # process_trajectory calls.
        await asyncio.sleep(0)
        async with execution_lock:
            executions.append(step.step_number)
        return {"executed_by": step.step_number}

    await asyncio.gather(
        process_trajectory(
            str(trajectory_id),
            session_factory=session_factory,
            redis_client=AsyncMock(),
            tool_executor=executor,
            worker_id="worker-a",
        ),
        process_trajectory(
            str(trajectory_id),
            session_factory=session_factory,
            redis_client=AsyncMock(),
            tool_executor=executor,
            worker_id="worker-b",
        ),
    )

    # Each of the three pending steps must have been executed exactly once,
    # never twice, regardless of which worker won the claim.
    assert sorted(executions) == [1, 2, 3]

    async with session_factory() as session:
        steps_result = await session.execute(
            select(ExecutionStep)
            .where(ExecutionStep.trajectory_id == trajectory_id)
            .order_by(ExecutionStep.step_number.asc())
        )
        steps = list(steps_result.scalars().all())
        trajectory_result = await session.execute(
            select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
        )
        trajectory = trajectory_result.scalar_one()

    assert [step.status for step in steps] == [
        StepStatus.COMPLETED.value,
        StepStatus.COMPLETED.value,
        StepStatus.COMPLETED.value,
    ]
    assert trajectory.status == WorkflowStatus.COMPLETED.value


@pytest.mark.asyncio()
async def test_healthz_reports_ok_when_database_and_redis_are_reachable(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr("app.main.make_redis_client", lambda: AsyncMock())

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/healthz")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "ok", "checks": {"database": "ok", "redis": "ok"}}


@pytest.mark.asyncio()
async def test_healthz_reports_degraded_when_redis_is_unreachable(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            yield session

    def broken_redis_client() -> AsyncMock:
        client = AsyncMock()
        client.ping.side_effect = ConnectionError("redis unreachable")
        return client

    app.dependency_overrides[get_db] = override_get_db
    monkeypatch.setattr("app.main.make_redis_client", broken_redis_client)

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/healthz")
    finally:
        app.dependency_overrides.pop(get_db, None)

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "degraded", "checks": {"database": "ok", "redis": "error"}}
