"""FastAPI management ingress for Hydra Engine."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any, cast

from fastapi import Depends, FastAPI, HTTPException, Path, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import Base, engine, get_db
from app.models import ExecutionStep, StepStatus, WorkflowStatus, WorkflowTrajectory
from app.worker import RedisQueueClient, build_default_steps, enqueue_trajectory, make_redis_client

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


@asynccontextmanager
async def lifespan(_: FastAPI) -> Any:
    """Create database metadata during application startup."""

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(
    title="Hydra Engine",
    version="0.1.0",
    description="Fault-tolerant message-driven agent workflow engine.",
    lifespan=lifespan,
)


@app.post(
    "/workflows/trigger",
    response_model=WorkflowResponse,
    status_code=status.HTTP_202_ACCEPTED,
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

    trajectory.status = WorkflowStatus.PENDING.value
    await db.commit()
    await db.refresh(trajectory)

    redis_client = make_redis_client()
    await enqueue_trajectory(cast(RedisQueueClient, redis_client), trajectory.id)
    await redis_client.aclose()

    return WorkflowResponse.model_validate(trajectory, from_attributes=True)


@app.get("/workflows/{id}/audit", response_model=WorkflowAuditResponse)
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
