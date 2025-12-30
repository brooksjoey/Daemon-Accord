from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..control_plane.models import Job, JobStatus
from ..control_plane.queue_manager import QueueManager
from ..db.models_worker import JobAttempt, JobLease, utcnow

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/internal/worker", tags=["internal-worker"])


def _utcnow_naive() -> datetime:
    # The canonical Job model uses naive UTC datetimes today.
    return datetime.utcnow()


# -------------------------
# Requests / Responses
# -------------------------


class ClaimRequest(BaseModel):
    worker_id: str = Field(min_length=1)
    streams: List[str] = Field(min_length=1)  # priority order
    max_wait_ms: int = Field(default=5000, ge=0, le=30000)


class ClaimResponse(BaseModel):
    claimed: bool
    job_id: Optional[str] = None
    attempt_id: Optional[UUID] = None
    lease_token: Optional[UUID] = None
    lease_ttl_seconds: Optional[int] = None
    heartbeat_interval_seconds: Optional[int] = None
    stream: Optional[str] = None
    message_id: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class HeartbeatRequest(BaseModel):
    worker_id: str
    job_id: str
    lease_token: UUID


class HeartbeatResponse(BaseModel):
    ok: bool
    lease_expires_at: Optional[datetime] = None


class CompleteRequest(BaseModel):
    worker_id: str
    job_id: str
    attempt_id: UUID
    lease_token: UUID
    stream: str
    message_id: str
    result: Dict[str, Any]


class FailError(BaseModel):
    code: str
    message: str
    stack: Optional[str] = None
    retryable: bool = True


class FailRequest(BaseModel):
    worker_id: str
    job_id: str
    attempt_id: UUID
    lease_token: UUID
    stream: str
    message_id: str
    error: FailError


class FinalizeResponse(BaseModel):
    ok: bool
    ack: bool
    requeued: bool = False
    dlq: bool = False


# -------------------------
# Settings
# -------------------------


@dataclass(frozen=True)
class WorkerLeaseSettings:
    lease_ttl_seconds: int = 60
    heartbeat_interval_seconds: int = 20


def _settings_from_app(request: Request) -> WorkerLeaseSettings:
    s = getattr(request.app.state, "settings", None)
    if not s:
        return WorkerLeaseSettings()
    return WorkerLeaseSettings(
        lease_ttl_seconds=getattr(s, "worker_lease_ttl_seconds", 60),
        heartbeat_interval_seconds=getattr(s, "worker_heartbeat_interval_seconds", 20),
    )


# -------------------------
# Dependencies
# -------------------------


async def get_session(request: Request):
    db = getattr(request.app.state, "db", None)
    if db is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="DB not initialized")
    async with db.session() as session:
        yield session


def get_queue_manager(request: Request) -> QueueManager:
    qm = getattr(request.app.state, "queue_manager", None)
    if qm is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="QueueManager not initialized")
    return qm


# -------------------------
# Helpers
# -------------------------


async def _require_active_lease(
    session: AsyncSession,
    job_id: str,
    worker_id: str,
    lease_token: UUID,
    attempt_id: Optional[UUID] = None,
) -> JobLease:
    lease = await session.get(JobLease, job_id)
    if not lease:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="No active lease for job")
    if lease.worker_id != worker_id or lease.lease_token != lease_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid lease token")
    if attempt_id and lease.attempt_id != attempt_id:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Attempt mismatch for lease")
    if lease.expires_at <= utcnow():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Lease expired")
    return lease


def _job_payload_snapshot(job: Job) -> Dict[str, Any]:
    # Workers expect an execution snapshot. Keep stable keys even if the Job table evolves.
    import json

    payload_dict: Dict[str, Any] = {}
    try:
        if job.payload:
            payload_dict = json.loads(job.payload) if isinstance(job.payload, str) else dict(job.payload)
    except Exception:
        payload_dict = {}

    return {
        "id": str(job.id),
        "domain": job.domain,
        "url": job.url,
        "type": job.job_type,
        "strategy": job.strategy,
        "payload": payload_dict,
        "priority": job.priority,
        "timeout_seconds": job.timeout_seconds,
    }


# -------------------------
# Endpoints
# -------------------------


@router.post("/claim", response_model=ClaimResponse)
async def claim_job(
    req: ClaimRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    qm: QueueManager = Depends(get_queue_manager),
) -> ClaimResponse:
    """
    Long-polls Redis Streams for a message, then atomically:
      - verifies job is claimable
      - creates JobAttempt
      - creates JobLease (exclusive)
      - sets job.status = RUNNING

    IMPORTANT: Does NOT ACK the Redis message. The worker must ACK only after
    Control Plane commits completion/failure and returns ack=true.
    """
    settings = _settings_from_app(request)

    msg = await qm.read_one_from_any_stream(
        streams=req.streams,
        max_wait_ms=req.max_wait_ms,
        consumer=req.worker_id,
    )
    if msg is None:
        return ClaimResponse(claimed=False)

    stream, message_id, fields = msg
    job_id = fields.get("job_id")
    if not job_id:
        # Poison message (missing required job_id). Ack to avoid wedging stream.
        await qm.ack(stream, message_id)
        return ClaimResponse(claimed=False)

    # Transaction: lease + attempt + job state
    async with session.begin():
        job: Job | None = await session.get(Job, job_id, with_for_update=True)
        if not job:
            # Job deleted; safe to ack and move on.
            # (No DB change required because canonical record is gone.)
            await qm.ack(stream, message_id)
            return ClaimResponse(claimed=False)

        # If job already terminal, safe to ack and move on.
        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            await qm.ack(stream, message_id)
            return ClaimResponse(claimed=False)

        existing_lease = await session.get(JobLease, job_id)
        if existing_lease and existing_lease.expires_at > utcnow():
            # Another worker owns it; do not ack. Let it be reclaimed/reaped later.
            return ClaimResponse(claimed=False)

        # Replace lease if expired.
        if existing_lease:
            await session.delete(existing_lease)
            await session.flush()

        attempt_no = int(job.attempts or 0) + 1
        attempt = JobAttempt(job_id=str(job.id), attempt_no=attempt_no, worker_id=req.worker_id)
        session.add(attempt)
        await session.flush()  # allocate attempt.id

        lease = JobLease.new(
            job_id=str(job.id),
            worker_id=req.worker_id,
            attempt_id=attempt.id,
            ttl_seconds=settings.lease_ttl_seconds,
        )
        session.add(lease)

        job.attempts = attempt_no
        job.status = JobStatus.RUNNING
        if not job.started_at:
            job.started_at = _utcnow_naive()
        # Add updated_at if present (added via migration); tolerate older DBs.
        if hasattr(job, "updated_at"):
            setattr(job, "updated_at", _utcnow_naive())

        payload = _job_payload_snapshot(job)

    logger.info(
        "job_claimed",
        job_id=str(job.id),
        worker_id=req.worker_id,
        attempt_id=str(attempt.id),
        stream=stream,
        message_id=message_id,
    )

    return ClaimResponse(
        claimed=True,
        job_id=str(job.id),
        attempt_id=attempt.id,
        lease_token=lease.lease_token,
        lease_ttl_seconds=settings.lease_ttl_seconds,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        stream=stream,
        message_id=message_id,
        payload=payload,
    )


@router.post("/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(
    req: HeartbeatRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> HeartbeatResponse:
    settings = _settings_from_app(request)
    async with session.begin():
        lease = await _require_active_lease(session, req.job_id, req.worker_id, req.lease_token)
        lease.expires_at = utcnow() + timedelta(seconds=settings.lease_ttl_seconds)
        session.add(lease)
    return HeartbeatResponse(ok=True, lease_expires_at=lease.expires_at)


@router.post("/complete", response_model=FinalizeResponse)
async def complete(
    req: CompleteRequest,
    session: AsyncSession = Depends(get_session),
) -> FinalizeResponse:
    """
    Idempotent completion guarded by lease_token and attempt_id.
    """
    async with session.begin():
        lease = await _require_active_lease(session, req.job_id, req.worker_id, req.lease_token, attempt_id=req.attempt_id)

        job: Job | None = await session.get(Job, req.job_id, with_for_update=True)
        if job is None:
            # Job removed; nothing to persist. Worker should still ACK transport.
            await session.delete(lease)
            return FinalizeResponse(ok=True, ack=True)

        # Idempotency: if already completed, allow ACK and ensure lease is cleared.
        if job.status == JobStatus.COMPLETED:
            await session.delete(lease)
            return FinalizeResponse(ok=True, ack=True)

        import json

        job.status = JobStatus.COMPLETED
        job.error = None
        job.completed_at = _utcnow_naive()
        if hasattr(job, "updated_at"):
            setattr(job, "updated_at", _utcnow_naive())
        job.result = json.dumps(req.result)

        attempt = await session.get(JobAttempt, req.attempt_id, with_for_update=True)
        if attempt:
            attempt.outcome = "succeeded"
            attempt.finished_at = utcnow()
            attempt.meta = attempt.meta or {}
            session.add(attempt)

        await session.delete(lease)
    return FinalizeResponse(ok=True, ack=True)


@router.post("/fail", response_model=FinalizeResponse)
async def fail(
    req: FailRequest,
    session: AsyncSession = Depends(get_session),
    qm: QueueManager = Depends(get_queue_manager),
) -> FinalizeResponse:
    """
    Failure handling with retry/DLQ decision made by Control Plane.
    Worker must ACK original message only when ack=true.
    """
    should_retry = False
    should_dlq = False

    async with session.begin():
        lease = await _require_active_lease(session, req.job_id, req.worker_id, req.lease_token, attempt_id=req.attempt_id)
        job: Job | None = await session.get(Job, req.job_id, with_for_update=True)
        if job is None:
            await session.delete(lease)
            return FinalizeResponse(ok=True, ack=True)

        attempt = await session.get(JobAttempt, req.attempt_id, with_for_update=True)
        if attempt:
            attempt.outcome = "failed"
            attempt.finished_at = utcnow()
            attempt.error_code = req.error.code
            attempt.error_message = req.error.message
            attempt.error_stack = req.error.stack
            session.add(attempt)

        attempts = int(job.attempts or 1)
        should_retry = bool(req.error.retryable) and attempts < int(job.max_attempts or 0)
        should_dlq = not should_retry

        # Release lease first (so reaper/reconcile can recover if enqueue/dlq fails).
        await session.delete(lease)

        if should_retry:
            job.status = JobStatus.QUEUED
            job.error = req.error.message
            if hasattr(job, "updated_at"):
                setattr(job, "updated_at", _utcnow_naive())
        else:
            job.status = JobStatus.FAILED
            job.error = req.error.message
            job.completed_at = _utcnow_naive()
            if hasattr(job, "updated_at"):
                setattr(job, "updated_at", _utcnow_naive())

    # Transport actions AFTER commit:
    if should_retry:
        try:
            await qm.enqueue(job_id=str(req.job_id), priority=int(job.priority), domain=str(job.domain), job_data=_job_payload_snapshot(job))
            return FinalizeResponse(ok=True, ack=True, requeued=True, dlq=False)
        except Exception as e:
            logger.error("requeue_failed", job_id=str(req.job_id), error=str(e))
            # Do not ack; message remains pending and will be reconciled.
            return FinalizeResponse(ok=False, ack=False, requeued=False, dlq=False)

    # DLQ path (required when not retrying)
    try:
        await qm.dlq_add({"job_id": str(req.job_id), "reason": req.error.code, "message": req.error.message})
        return FinalizeResponse(ok=True, ack=True, requeued=False, dlq=True)
    except Exception as e:
        logger.error("dlq_failed", job_id=str(req.job_id), error=str(e))
        return FinalizeResponse(ok=False, ack=False, requeued=False, dlq=False)

