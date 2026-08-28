"""Durable execution and state replay worker core."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Protocol, cast

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.database import AsyncSessionFactory
from app.models import ExecutionStep, StepStatus, StepType, WorkflowStatus, WorkflowTrajectory

logger = logging.getLogger("hydra_engine.worker")
RedisValue = bytes | bytearray | memoryview | str | int | float
RedisPopValue = bytes | str
RedisPopResult = tuple[RedisPopValue, RedisPopValue] | list[RedisPopValue] | None


class RedisQueueClient(Protocol):
    """Minimal async Redis queue surface used by the worker."""

    def lpush(self, name: RedisValue, *values: RedisValue) -> Awaitable[int]:
        """Push values onto a Redis list."""

    def zadd(self, name: RedisValue, mapping: dict[RedisValue, float]) -> Awaitable[int]:
        """Add delayed task values into a Redis sorted set."""

    def brpop(
        self,
        keys: RedisValue | Iterable[RedisValue],
        timeout: int | float | None = 0,
    ) -> Awaitable[RedisPopResult]:
        """Pop a queued task, blocking up to timeout seconds."""


ToolExecutor = Callable[[ExecutionStep, list[dict[str, Any]]], Awaitable[dict[str, Any]]]


async def default_tool_executor(
    step: ExecutionStep,
    replay_state: list[dict[str, Any]],
) -> dict[str, Any]:
    """Execute a deterministic built-in step for local and test deployments."""

    await asyncio.sleep(0)
    payload = dict(step.input_payload)
    if payload.get("force_error") is True:
        message = str(payload.get("error_message", "forced step failure"))
        raise RuntimeError(message)
    if settings.HYDRA_USE_GATEWAY_MODEL and "prompt" in payload:
        return await _execute_gateway_prompt(payload, replay_state)
    return {
        "step_id": str(step.id),
        "step_number": step.step_number,
        "step_type": step.step_type,
        "input": payload,
        "replay_state_size": len(replay_state),
    }


async def _execute_gateway_prompt(
    payload: dict[str, Any],
    replay_state: list[dict[str, Any]],
) -> dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "You are the execution planner inside Hydra Engine. Return concise JSON-like text."
            ),
        },
        {
            "role": "user",
            "content": str(payload["prompt"]),
        },
    ]
    if replay_state:
        messages.insert(
            1,
            {
                "role": "system",
                "content": f"Replay state: {json.dumps(replay_state, default=str)}",
            },
        )
    headers = {
        "X-Tenant-ID": settings.HYDRA_GATEWAY_TENANT_ID,
        "Authorization": f"Bearer {settings.HYDRA_GATEWAY_BEARER_TOKEN}",
    }
    request_payload = {
        "model": str(payload.get("model") or settings.HYDRA_GATEWAY_MODEL),
        "messages": messages,
        "temperature": float(payload.get("temperature", 0.2)),
        "max_tokens": int(payload.get("max_tokens", 512)),
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            settings.HYDRA_GATEWAY_URL,
            json=request_payload,
            headers=headers,
        )
        response.raise_for_status()
        return {
            "gateway_url": settings.HYDRA_GATEWAY_URL,
            "model": request_payload["model"],
            "response": response.json(),
        }


def make_redis_client(redis_url: str | None = None) -> Redis:
    """Create a Redis asyncio client from settings."""

    return Redis.from_url(redis_url or settings.REDIS_URL, decode_responses=False)


async def enqueue_trajectory(
    redis_client: RedisQueueClient,
    trajectory_id: uuid.UUID | str,
    *,
    delay_seconds: float = 0.0,
) -> None:
    """Queue a workflow trajectory for immediate or delayed execution."""

    payload = json.dumps({"trajectory_id": str(trajectory_id)})
    if delay_seconds > 0:
        run_at = time.time() + delay_seconds
        await redis_client.zadd(settings.REDIS_DELAYED_QUEUE_NAME, {payload: run_at})
        logger.info(
            "trajectory_requeued_with_delay",
            extra={"trajectory_id": str(trajectory_id), "delay_seconds": delay_seconds},
        )
        return
    await redis_client.lpush(settings.REDIS_QUEUE_NAME, payload)
    logger.info("trajectory_queued", extra={"trajectory_id": str(trajectory_id)})


async def process_trajectory(
    trajectory_id: str,
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionFactory,
    redis_client: RedisQueueClient | None = None,
    tool_executor: ToolExecutor = default_tool_executor,
) -> None:
    """Replay and process a workflow trajectory from durable state."""

    queue: RedisQueueClient = (
        redis_client if redis_client is not None else cast(RedisQueueClient, make_redis_client())
    )
    parsed_trajectory_id = uuid.UUID(trajectory_id)
    async with session_factory() as session:
        trajectory = await _load_trajectory(session, parsed_trajectory_id)
        if trajectory is None:
            logger.warning("trajectory_not_found", extra={"trajectory_id": trajectory_id})
            return

        trajectory.status = WorkflowStatus.RUNNING.value
        await session.commit()

        steps = await _load_steps(session, parsed_trajectory_id)
        replay_state: list[dict[str, Any]] = []

        for step in steps:
            if step.status == StepStatus.COMPLETED.value:
                replay_state.append(
                    {
                        "step_number": step.step_number,
                        "step_type": step.step_type,
                        "output_payload": step.output_payload,
                    }
                )
                continue

            if step.status not in {StepStatus.PENDING.value, StepStatus.FAILED.value}:
                continue

            if step.retry_count >= settings.MAX_STEP_RETRIES:
                step.status = StepStatus.FAILED.value
                trajectory.status = WorkflowStatus.FAILED.value
                await session.commit()
                logger.error(
                    "step_retry_limit_exceeded",
                    extra={"trajectory_id": trajectory_id, "step_number": step.step_number},
                )
                return

            try:
                output_payload = await tool_executor(step, replay_state)
            except Exception:
                await _record_step_failure(session, trajectory, step, queue)
                return

            step.output_payload = output_payload
            step.status = StepStatus.COMPLETED.value
            step.error_log = None
            replay_state.append(
                {
                    "step_number": step.step_number,
                    "step_type": step.step_type,
                    "output_payload": output_payload,
                }
            )
            await session.commit()

        trajectory.status = WorkflowStatus.COMPLETED.value
        await session.commit()
        logger.info("trajectory_completed", extra={"trajectory_id": trajectory_id})


async def run_worker_forever(
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionFactory,
    redis_client: RedisQueueClient | None = None,
    tool_executor: ToolExecutor = default_tool_executor,
    poll_timeout_seconds: int = 5,
) -> None:
    """Continuously consume workflow tasks from Redis."""

    queue: RedisQueueClient = (
        redis_client if redis_client is not None else cast(RedisQueueClient, make_redis_client())
    )
    while True:
        try:
            item = await queue.brpop(settings.REDIS_QUEUE_NAME, poll_timeout_seconds)
        except RedisTimeoutError:
            continue
        except RedisError:
            logger.exception("redis_queue_poll_failed")
            await asyncio.sleep(1.0)
            continue
        if item is None:
            continue
        raw_payload = _extract_brpop_payload(item)
        payload_text = _decode_redis_value(raw_payload)
        payload = json.loads(payload_text)
        await process_trajectory(
            str(payload["trajectory_id"]),
            session_factory=session_factory,
            redis_client=queue,
            tool_executor=tool_executor,
        )


async def _load_trajectory(
    session: AsyncSession,
    trajectory_id: uuid.UUID,
) -> WorkflowTrajectory | None:
    result = await session.execute(
        select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
    )
    return result.scalar_one_or_none()


async def _load_steps(session: AsyncSession, trajectory_id: uuid.UUID) -> list[ExecutionStep]:
    result = await session.execute(
        select(ExecutionStep)
        .where(ExecutionStep.trajectory_id == trajectory_id)
        .order_by(ExecutionStep.step_number.asc())
    )
    return list(result.scalars().all())


def _extract_brpop_payload(item: RedisPopResult) -> RedisPopValue:
    if item is None:
        raise ValueError("Cannot extract payload from an empty Redis pop result.")
    if len(item) != 2:
        raise ValueError(f"Unexpected Redis pop result shape: {item!r}")
    return item[1]


def _decode_redis_value(value: RedisPopValue) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


async def _record_step_failure(
    session: AsyncSession,
    trajectory: WorkflowTrajectory,
    step: ExecutionStep,
    queue: RedisQueueClient,
) -> None:
    step.retry_count += 1
    step.status = StepStatus.FAILED.value
    step.error_log = traceback.format_exc()

    if step.retry_count >= settings.MAX_STEP_RETRIES:
        trajectory.status = WorkflowStatus.FAILED.value
        await session.commit()
        logger.exception(
            "step_failed_permanently",
            extra={
                "trajectory_id": str(trajectory.id),
                "step_number": step.step_number,
                "retry_count": step.retry_count,
            },
        )
        return

    trajectory.status = WorkflowStatus.PENDING.value
    await session.commit()
    delay_seconds = settings.BACKOFF_FACTOR**step.retry_count
    await enqueue_trajectory(queue, trajectory.id, delay_seconds=delay_seconds)
    logger.exception(
        "step_failed_requeued",
        extra={
            "trajectory_id": str(trajectory.id),
            "step_number": step.step_number,
            "retry_count": step.retry_count,
            "delay_seconds": delay_seconds,
        },
    )


def build_default_steps(
    trajectory_id: uuid.UUID,
    step_inputs: list[dict[str, Any]],
) -> list[ExecutionStep]:
    """Build sequential pending steps from request input."""

    if not step_inputs:
        step_inputs = [
            {"prompt": "plan next action"},
            {"tool": "mock_api", "arguments": {}},
            {"result_consumer": "agent_state"},
        ]

    steps: list[ExecutionStep] = []
    step_types = [
        StepType.LLM_THOUGHT.value,
        StepType.TOOL_CALL.value,
        StepType.TOOL_RESPONSE.value,
    ]
    for index, input_payload in enumerate(step_inputs, start=1):
        steps.append(
            ExecutionStep(
                trajectory_id=trajectory_id,
                step_number=index,
                step_type=step_types[(index - 1) % len(step_types)],
                input_payload=input_payload,
                status=StepStatus.PENDING.value,
            )
        )
    return steps
