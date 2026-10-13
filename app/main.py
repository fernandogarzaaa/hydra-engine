"""FastAPI management ingress for Hydra Engine."""

from __future__ import annotations

import hmac
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, cast

from fastapi import Depends, FastAPI, Header, HTTPException, Path, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import Base, engine, get_db
from app.models import ExecutionStep, StepStatus, WorkflowStatus, WorkflowTrajectory
from app.worker import RedisQueueClient, build_default_steps, enqueue_trajectory, make_redis_client

logger = logging.getLogger("hydra_engine.api")


async def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """Optional API-key auth. Off when HYDRA_API_KEY is unset (local/dev use);
    when set, every workflow route needs the key via X-API-Key or Bearer."""

    expected = settings.HYDRA_API_KEY
    if not expected:
        return
    supplied = x_api_key or ""
    if not supplied and authorization and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if not supplied or not hmac.compare_digest(supplied.encode(), expected.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )


ApiKey = Depends(require_api_key)
DbSession = Annotated[AsyncSession, Depends(get_db)]
WorkflowIdPath = Annotated[uuid.UUID, Path(description="Workflow trajectory identifier.")]


class TriggerWorkflowRequest(BaseModel):
    """Request body for creating a workflow trajectory."""

    tenant_id: str = Field(min_length=1, max_length=128)
    steps: list[dict[str, Any]] = Field(default_factory=list)


class WorkflowResponse(BaseModel):
    """Tracking metadata for workflow operations."""

    id: uuid.UUID
    tenant_id: str
    status: str
    created_at: datetime
    updated_at: datetime


class StepAuditResponse(BaseModel):
    """Audit timeline response for a single execution step."""

    id: uuid.UUID
    step_number: int
    step_type: str
    status: str
    retry_count: int
    input_payload: dict[str, Any]
    output_payload: dict[str, Any] | None
    error_log: str | None
    elapsed_seconds: float
    created_at: datetime
    updated_at: datetime


class WorkflowAuditResponse(BaseModel):
    """Complete workflow audit response."""

    id: uuid.UUID
    tenant_id: str
    status: str
    total_retries: int
    timeline: list[StepAuditResponse]


class HealthResponse(BaseModel):
    """Liveness/readiness response for the health check endpoint."""

    status: str
    checks: dict[str, str]


@asynccontextmanager
async def lifespan(_: FastAPI) -> Any:
    """Create database metadata during application startup."""

    if not settings.HYDRA_API_KEY:
        logger.warning(
            "HYDRA_API_KEY is not set: the workflow API is unauthenticated. "
            "Bind to 127.0.0.1 (uvicorn's default) or set HYDRA_API_KEY."
        )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(
    title="Hydra Engine",
    version="0.1.1",
    description="Fault-tolerant message-driven agent workflow engine.",
    lifespan=lifespan,
)


@app.get("/healthz", response_model=HealthResponse)
async def healthz(db: DbSession) -> HealthResponse:
    """Report liveness and cheap connectivity to the app's Postgres and Redis dependencies."""

    checks: dict[str, str] = {}

    try:
        await db.execute(select(1))
        checks["database"] = "ok"
    except Exception:
        checks["database"] = "error"

    redis_client = make_redis_client()
    try:
        await redis_client.ping()
        checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "error"
    finally:
        await redis_client.aclose()

    overall_status = "ok" if all(value == "ok" for value in checks.values()) else "degraded"
    return HealthResponse(status=overall_status, checks=checks)


@app.post(
    "/workflows/trigger",
    response_model=WorkflowResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[ApiKey],
)
async def trigger_workflow(
    request: TriggerWorkflowRequest,
    db: DbSession,
) -> WorkflowResponse:
    """Create a workflow trajectory and enqueue it for worker execution."""

    trajectory = WorkflowTrajectory(
        tenant_id=request.tenant_id,
        status=WorkflowStatus.PENDING.value,
    )
    db.add(trajectory)
    await db.flush()

    steps = build_default_steps(trajectory.id, request.steps)
    db.add_all(steps)
    await db.commit()
    await db.refresh(trajectory)

    redis_client = make_redis_client()
    await enqueue_trajectory(cast(RedisQueueClient, redis_client), trajectory.id)
    await redis_client.aclose()

    return WorkflowResponse.model_validate(trajectory, from_attributes=True)


@app.post(
    "/workflows/{id}/resume",
    response_model=WorkflowResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[ApiKey],
)
async def resume_workflow(
    id: WorkflowIdPath,
    db: DbSession,
) -> WorkflowResponse:
    """Resume a failed or stalled workflow by re-enqueueing pending work."""

    trajectory = await _get_trajectory_or_404(db, id)
    steps = await _get_steps(db, id)
    reset_started = False
    for step in steps:
        if step.status != StepStatus.COMPLETED.value:
            reset_started = True
        if reset_started:
            step.status = StepStatus.PENDING.value
            step.error_log = None
            # A resume is an explicit operator decision to try again: give the
            # step a fresh retry budget, otherwise an exhausted step fails again
            # immediately without running.
            step.retry_count = 0

    trajectory.status = WorkflowStatus.PENDING.value
    await db.commit()
    await db.refresh(trajectory)

    redis_client = make_redis_client()
    await enqueue_trajectory(cast(RedisQueueClient, redis_client), trajectory.id)
    await redis_client.aclose()

    return WorkflowResponse.model_validate(trajectory, from_attributes=True)


@app.get(
    "/workflows/{id}/audit",
    response_model=WorkflowAuditResponse,
    dependencies=[ApiKey],
)
async def audit_workflow(
    id: WorkflowIdPath,
    db: DbSession,
) -> WorkflowAuditResponse:
    """Return complete workflow execution history for debugging."""

    trajectory = await _get_trajectory_or_404(db, id)
    steps = await _get_steps(db, id)
    timeline = [
        StepAuditResponse(
            id=step.id,
            step_number=step.step_number,
            step_type=step.step_type,
            status=step.status,
            retry_count=step.retry_count,
            input_payload=step.input_payload,
            output_payload=step.output_payload,
            error_log=step.error_log,
            elapsed_seconds=max(0.0, (step.updated_at - step.created_at).total_seconds()),
            created_at=step.created_at,
            updated_at=step.updated_at,
        )
        for step in steps
    ]
    return WorkflowAuditResponse(
        id=trajectory.id,
        tenant_id=trajectory.tenant_id,
        status=trajectory.status,
        total_retries=sum(step.retry_count for step in steps),
        timeline=timeline,
    )


async def _get_trajectory_or_404(db: AsyncSession, trajectory_id: uuid.UUID) -> WorkflowTrajectory:
    result = await db.execute(
        select(WorkflowTrajectory).where(WorkflowTrajectory.id == trajectory_id)
    )
    trajectory = result.scalar_one_or_none()
    if trajectory is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow trajectory {trajectory_id} was not found.",
        )
    return trajectory


async def _get_steps(db: AsyncSession, trajectory_id: uuid.UUID) -> list[ExecutionStep]:
    result = await db.execute(
        select(ExecutionStep)
        .where(ExecutionStep.trajectory_id == trajectory_id)
        .order_by(ExecutionStep.step_number.asc())
    )
    return list(result.scalars().all())
