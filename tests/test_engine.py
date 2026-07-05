"""Integration validation suite for Hydra Engine."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models import ExecutionStep, StepStatus, StepType, WorkflowStatus, WorkflowTrajectory
from app.worker import process_trajectory


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
                        StepType.TOOL_CALL.value
                        if step_number == 3
                        else StepType.LLM_THOUGHT.value
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
