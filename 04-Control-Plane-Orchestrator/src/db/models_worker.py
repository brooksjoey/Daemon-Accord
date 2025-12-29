from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from sqlmodel import SQLModel, Field, Column, JSON


def utcnow() -> datetime:
    # Keep timezone-aware timestamps for lease correctness.
    return datetime.now(timezone.utc)


class JobAttempt(SQLModel, table=True):
    """
    Tracks one execution attempt for a job.

    NOTE: `job_id` is stored as string UUID because the canonical `jobs.id`
    in this repo is currently a string.
    """

    __tablename__ = "job_attempts"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)
    job_id: str = Field(index=True)

    attempt_no: int = Field(default=1, index=True)

    worker_id: str = Field(index=True)
    started_at: datetime = Field(default_factory=utcnow, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)

    # "succeeded" / "failed"
    outcome: Optional[str] = Field(default=None, index=True)

    error_code: Optional[str] = Field(default=None, index=True)
    error_message: Optional[str] = Field(default=None)
    error_stack: Optional[str] = Field(default=None)

    # Optional: store timing summary, diagnostics
    meta: Dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class JobLease(SQLModel, table=True):
    """
    Exclusive per-job lease with TTL.

    NOTE: `job_id` is stored as string UUID because the canonical `jobs.id`
    in this repo is currently a string.
    """

    __tablename__ = "job_leases"

    job_id: str = Field(primary_key=True)
    lease_token: UUID = Field(default_factory=uuid4, index=True)

    worker_id: str = Field(index=True)

    acquired_at: datetime = Field(default_factory=utcnow, index=True)
    expires_at: datetime = Field(index=True)

    attempt_id: UUID = Field(index=True)

    @staticmethod
    def new(job_id: str, worker_id: str, attempt_id: UUID, ttl_seconds: int) -> "JobLease":
        return JobLease(
            job_id=job_id,
            worker_id=worker_id,
            attempt_id=attempt_id,
            expires_at=utcnow() + timedelta(seconds=ttl_seconds),
        )

