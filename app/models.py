"""Workflow state-machine persistence models."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON, TypeDecorator

from app.database import Base


class StepStatus(StrEnum):
    """Execution status for workflow steps."""

    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkflowStatus(StrEnum):
    """Macro workflow trajectory status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class StepType(StrEnum):
    """Supported step kinds for agent workflow execution."""

    LLM_THOUGHT = "LLM_THOUGHT"
    TOOL_CALL = "TOOL_CALL"
    TOOL_RESPONSE = "TOOL_RESPONSE"


class PortableUUID(TypeDecorator[uuid.UUID]):
    """Use PostgreSQL UUID in production and a portable string UUID elsewhere."""

    impl = String(36)
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID(as_uuid=True))
        return dialect.type_descriptor(String(36))

    def process_bind_param(
        self,
        value: uuid.UUID | str | None,
        dialect: Any,
    ) -> uuid.UUID | str | None:
        if value is None:
            return None
        parsed = value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        if dialect.name == "postgresql":
            return parsed
        return str(parsed)

    def process_result_value(self, value: uuid.UUID | str | None, dialect: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


class PortableJSONB(TypeDecorator[dict[str, Any]]):
    """Use PostgreSQL JSONB in production and generic JSON for tests."""

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())


def utc_now() -> datetime:
    """Return the current UTC timestamp."""

    return datetime.now(UTC)


class WorkflowTrajectory(Base):
    """Macro execution state for a workflow trajectory."""

    __tablename__ = "workflow_trajectories"

    id: Mapped[uuid.UUID] = mapped_column(PortableUUID(), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=WorkflowStatus.PENDING.value,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )

    steps: Mapped[list[ExecutionStep]] = relationship(
        "ExecutionStep",
        back_populates="trajectory",
        cascade="all, delete-orphan",
        order_by="ExecutionStep.step_number",
        lazy="selectin",
    )


class ExecutionStep(Base):
    """Micro execution state for an individual workflow step."""

    __tablename__ = "execution_steps"
    __table_args__ = (
        UniqueConstraint(
            "trajectory_id",
            "step_number",
            name="uq_execution_step_trajectory_number",
        ),
        Index("ix_execution_steps_trajectory_step", "trajectory_id", "step_number"),
    )

    id: Mapped[uuid.UUID] = mapped_column(PortableUUID(), primary_key=True, default=uuid.uuid4)
    trajectory_id: Mapped[uuid.UUID] = mapped_column(
        PortableUUID(),
        ForeignKey("workflow_trajectories.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    step_type: Mapped[str] = mapped_column(String(32), nullable=False)
    input_payload: Mapped[dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(PortableJSONB()),
        nullable=False,
        default=dict,
    )
    output_payload: Mapped[dict[str, Any] | None] = mapped_column(
        MutableDict.as_mutable(PortableJSONB()),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=StepStatus.PENDING.value,
        index=True,
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_log: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )

    trajectory: Mapped[WorkflowTrajectory] = relationship(
        "WorkflowTrajectory",
        back_populates="steps",
        lazy="selectin",
    )
