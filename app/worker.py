"""Durable execution and state replay worker core."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import socket
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable
from datetime import timedelta
from typing import Any, Protocol, cast

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import CursorResult, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.database import AsyncSessionFactory
from app.models import (
    ExecutionStep,
    StepStatus,
    StepType,
    WorkflowStatus,
    WorkflowTrajectory,
    utc_now,
)

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

    def zrangebyscore(
        self,
        name: RedisValue,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> Awaitable[list[RedisPopValue]]:
        """Read delayed task values whose score falls in [min, max]."""

    def zrem(self, name: RedisValue, *values: RedisValue) -> Awaitable[int]:
        """Remove values from a Redis sorted set; returns how many were removed."""


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


async def promote_due_retries(
    redis_client: RedisQueueClient,
    *,
    now: float | None = None,
    batch_size: int = 100,
) -> int:
    """Move delayed retries whose run-at time has passed onto the work queue.

    Safe with several workers running the same loop: an entry is pushed only
    by the caller whose ``ZREM`` actually removed it, so each due retry is
    enqueued exactly once. Returns the number of entries promoted.
    """

    cutoff = time.time() if now is None else now
    due = await redis_client.zrangebyscore(
        settings.REDIS_DELAYED_QUEUE_NAME, "-inf", cutoff, start=0, num=batch_size
    )
    promoted = 0
    for raw in due:
        if await redis_client.zrem(settings.REDIS_DELAYED_QUEUE_NAME, raw) != 1:
            continue  # another worker promoted it first
        await redis_client.lpush(settings.REDIS_QUEUE_NAME, raw)
        promoted += 1
    if promoted:
        logger.info("delayed_retries_promoted", extra={"count": promoted})
    return promoted


def _default_worker_id() -> str:
    """Build a reasonably unique identifier for this worker process."""

    return f"{socket.gethostname()}:{uuid.uuid4()}"


async def process_trajectory(
    trajectory_id: str,
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionFactory,
    redis_client: RedisQueueClient | None = None,
    tool_executor: ToolExecutor = default_tool_executor,
    worker_id: str | None = None,
) -> None:
    """Replay and process a workflow trajectory from durable state.

    Before doing any work, the trajectory is atomically claimed via a single
    ``UPDATE ... WHERE`` statement so that when two worker processes poll the
    same trajectory concurrently, only one of them wins the claim and
    proceeds; the other observes zero rows affected and returns immediately
    without executing (and thus without double-executing) any steps.
    """

    queue: RedisQueueClient = (
        redis_client if redis_client is not None else cast(RedisQueueClient, make_redis_client())
    )
    resolved_worker_id = worker_id or _default_worker_id()
    parsed_trajectory_id = uuid.UUID(trajectory_id)
    async with session_factory() as session:
        claimed = await _claim_trajectory(
            session,
            parsed_trajectory_id,
            worker_id=resolved_worker_id,
            lease_seconds=settings.TRAJECTORY_LEASE_SECONDS,
        )
        if not claimed:
            await _requeue_if_leased_elsewhere(session, parsed_trajectory_id, queue)
            logger.info(
                "trajectory_claim_lost",
                extra={"trajectory_id": trajectory_id, "worker_id": resolved_worker_id},
            )
            return

        trajectory = await _load_trajectory(session, parsed_trajectory_id)
        if trajectory is None:
            logger.warning("trajectory_not_found", extra={"trajectory_id": trajectory_id})
            return

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
                if not await _confirm_ownership(session, parsed_trajectory_id, resolved_worker_id):
                    await _abandon_lost_lease(
                        session, parsed_trajectory_id, resolved_worker_id, "retry_limit"
                    )
                    return
                _clear_lease(trajectory)
                await session.commit()
                logger.error(
                    "step_retry_limit_exceeded",
                    extra={"trajectory_id": trajectory_id, "step_number": step.step_number},
                )
                return

            try:
                async with _lease_heartbeat(
                    session_factory, parsed_trajectory_id, resolved_worker_id
                ):
                    output_payload = await tool_executor(step, replay_state)
            except Exception:
                await _record_step_failure(
                    session, trajectory, step, queue, worker_id=resolved_worker_id
                )
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
            if not await _confirm_ownership(session, parsed_trajectory_id, resolved_worker_id):
                # Another worker owns the trajectory now; it will (re)run this
                # step. Committing here would record a second execution and
                # let both workers race through the remaining steps.
                await _abandon_lost_lease(
                    session, parsed_trajectory_id, resolved_worker_id, "step_commit"
                )
                return
            await session.commit()

        trajectory.status = WorkflowStatus.COMPLETED.value
        if not await _confirm_ownership(session, parsed_trajectory_id, resolved_worker_id):
            await _abandon_lost_lease(session, parsed_trajectory_id, resolved_worker_id, "complete")
            return
        _clear_lease(trajectory)
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
    block_seconds = max(1, min(poll_timeout_seconds, int(settings.DELAYED_POLL_SECONDS)))
    while True:
        try:
            await promote_due_retries(queue)
            item = await queue.brpop(settings.REDIS_QUEUE_NAME, block_seconds)
        except RedisTimeoutError:
            continue
        except RedisError:
            logger.exception("redis_queue_poll_failed")
            await asyncio.sleep(1.0)
            continue
        if item is None:
            continue
        await handle_queue_item(
            item,
            session_factory=session_factory,
            redis_client=queue,
            tool_executor=tool_executor,
        )


async def handle_queue_item(
    item: RedisPopResult,
    *,
    session_factory: async_sessionmaker[AsyncSession] = AsyncSessionFactory,
    redis_client: RedisQueueClient,
    tool_executor: ToolExecutor = default_tool_executor,
) -> bool:
    """Process one popped queue item. Never raises: a malformed message or a
    failure while processing is logged (malformed messages are parked on the
    dead-letter list) so one bad item cannot stop the worker loop."""

    try:
        raw_payload = _extract_brpop_payload(item)
        payload_text = _decode_redis_value(raw_payload)
        payload = json.loads(payload_text)
        trajectory_id = str(payload["trajectory_id"])
        uuid.UUID(trajectory_id)
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        logger.exception("queue_message_malformed")
        try:
            await redis_client.lpush(settings.REDIS_DEAD_LETTER_QUEUE_NAME, repr(item))
        except RedisError:
            logger.exception("dead_letter_push_failed")
        return False
    try:
        await process_trajectory(
            trajectory_id,
            session_factory=session_factory,
            redis_client=redis_client,
            tool_executor=tool_executor,
        )
    except Exception:
        logger.exception("trajectory_processing_crashed", extra={"trajectory_id": trajectory_id})
        return False
    return True


def _clear_lease(trajectory: WorkflowTrajectory) -> None:
    """Release this worker's claim so the next enqueue can be picked up at once."""

    trajectory.claimed_by = None
    trajectory.lease_expires_at = None


async def _renew_lease(
    session_factory: async_sessionmaker[AsyncSession],
    trajectory_id: uuid.UUID,
    worker_id: str,
    lease_seconds: float,
) -> bool:
    """Extend the lease, but only while this worker still holds the claim."""

    async with session_factory() as session:
        stmt = (
            update(WorkflowTrajectory)
            .where(
                WorkflowTrajectory.id == trajectory_id,
                WorkflowTrajectory.claimed_by == worker_id,
            )
            .values(lease_expires_at=utc_now() + timedelta(seconds=lease_seconds))
        )
        result = cast(CursorResult[Any], await session.execute(stmt))
        await session.commit()
        return bool(result.rowcount == 1)


class _lease_heartbeat:
    """Renew the trajectory lease in the background while a step runs, so a
    step that takes longer than TRAJECTORY_LEASE_SECONDS is not reclaimed and
    double-executed by another worker."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        trajectory_id: uuid.UUID,
        worker_id: str,
    ) -> None:
        self._session_factory = session_factory
        self._trajectory_id = trajectory_id
        self._worker_id = worker_id
        self._task: asyncio.Task[None] | None = None

    async def _beat(self) -> None:
        lease = settings.TRAJECTORY_LEASE_SECONDS
        interval = max(0.01, lease / 3.0)
        while True:
            await asyncio.sleep(interval)
            try:
                held = await _renew_lease(
                    self._session_factory, self._trajectory_id, self._worker_id, lease
                )
            except Exception:
                logger.exception(
                    "lease_renew_failed", extra={"trajectory_id": str(self._trajectory_id)}
                )
                continue
            if not held:
                logger.warning(
                    "lease_lost_during_step", extra={"trajectory_id": str(self._trajectory_id)}
                )
                return

    async def __aenter__(self) -> _lease_heartbeat:
        self._task = asyncio.create_task(self._beat())
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None


async def _requeue_if_leased_elsewhere(
    session: AsyncSession,
    trajectory_id: uuid.UUID,
    queue: RedisQueueClient,
) -> None:
    """A retry or resume can arrive while another worker still holds the lease.

    Dropping that message would lose the work, so park it on the delayed set
    until the current lease ends; the holder normally releases sooner, and the
    re-delivered message then claims (or finds the trajectory completed).
    """

    trajectory = await _load_trajectory(session, trajectory_id)
    if trajectory is None or trajectory.status == WorkflowStatus.COMPLETED.value:
        return
    lease_expires_at = trajectory.lease_expires_at
    if lease_expires_at is None:
        delay = 1.0
    else:
        if lease_expires_at.tzinfo is None:
            lease_expires_at = lease_expires_at.replace(tzinfo=utc_now().tzinfo)
        delay = max(1.0, (lease_expires_at - utc_now()).total_seconds())
    await enqueue_trajectory(queue, trajectory_id, delay_seconds=delay)


async def _confirm_ownership(
    session: AsyncSession,
    trajectory_id: uuid.UUID,
    worker_id: str,
) -> bool:
    """Fence a write: re-assert this worker's claim inside the caller's transaction.

    A conditional ``UPDATE`` (only while ``claimed_by`` is still this worker)
    that also extends the lease. On PostgreSQL it takes the row lock, so a
    concurrent reclaim waits for this transaction and then sees the fresh
    lease. If it matches no row the lease was lost (expired and reclaimed by
    another worker, or reset): the caller must roll back instead of committing
    step output or overwriting the new holder's claim.
    """

    stmt = (
        update(WorkflowTrajectory)
        .where(
            WorkflowTrajectory.id == trajectory_id,
            WorkflowTrajectory.claimed_by == worker_id,
        )
        .values(lease_expires_at=utc_now() + timedelta(seconds=settings.TRAJECTORY_LEASE_SECONDS))
        .execution_options(synchronize_session=False)
    )
    result = cast(CursorResult[Any], await session.execute(stmt))
    return bool(result.rowcount == 1)


async def _abandon_lost_lease(
    session: AsyncSession, trajectory_id: uuid.UUID, worker_id: str, where: str
) -> None:
    await session.rollback()
    logger.warning(
        "lease_lost_write_discarded",
        extra={"trajectory_id": str(trajectory_id), "worker_id": worker_id, "at": where},
    )


async def _claim_trajectory(
    session: AsyncSession,
    trajectory_id: uuid.UUID,
    *,
    worker_id: str,
    lease_seconds: float,
) -> bool:
    """Atomically claim a trajectory for exclusive processing by this worker.

    This closes a concurrency gap where two worker processes could both pull
    the same pending trajectory and double-execute its steps: the claim is a
    single ``UPDATE ... WHERE`` statement whose ``WHERE`` clause only matches
    a row that is unclaimed, or whose previous lease has expired (e.g. the
    worker holding it crashed). The database guarantees that at most one
    concurrent transaction can update a given row, so at most one caller can
    ever observe ``rowcount == 1`` for the same trajectory and lease window;
    every other concurrent caller observes ``rowcount == 0`` and must not
    proceed.
    """

    now = utc_now()
    stmt = (
        update(WorkflowTrajectory)
        .where(
            WorkflowTrajectory.id == trajectory_id,
            WorkflowTrajectory.status != WorkflowStatus.COMPLETED.value,
            or_(
                WorkflowTrajectory.lease_expires_at.is_(None),
                WorkflowTrajectory.lease_expires_at < now,
            ),
        )
        .values(
            status=WorkflowStatus.RUNNING.value,
            claimed_by=worker_id,
            claimed_at=now,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
        )
    )
    result = cast(CursorResult[Any], await session.execute(stmt))
    await session.commit()
    return bool(result.rowcount == 1)


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
    *,
    worker_id: str,
) -> None:
    step.retry_count += 1
    step.status = StepStatus.FAILED.value
    step.error_log = traceback.format_exc()
    if not await _confirm_ownership(session, trajectory.id, worker_id):
        # The lease moved to another worker mid-step; its run owns the retry
        # budget and the status. Do not overwrite them or schedule a retry.
        await _abandon_lost_lease(session, trajectory.id, worker_id, "step_failure")
        return

    if step.retry_count >= settings.MAX_STEP_RETRIES:
        trajectory.status = WorkflowStatus.FAILED.value
        _clear_lease(trajectory)
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
    # Release before scheduling the retry: otherwise the retry, if it is due
    # before the lease would have expired, loses the claim and is dropped.
    _clear_lease(trajectory)
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
