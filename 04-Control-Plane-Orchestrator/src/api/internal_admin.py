from __future__ import annotations

from datetime import datetime
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..control_plane.queue_manager import QueueManager
from ..db.models_worker import JobLease, utcnow

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/internal/admin", tags=["internal-admin"])


class ReconcileRequest(BaseModel):
    streams: Optional[List[str]] = Field(default=None, description="Streams to reconcile (defaults to priority streams)")
    pel_idle_threshold_seconds: int = Field(default=60, ge=1, le=3600)
    batch_size: int = Field(default=100, ge=1, le=1000)


class ReconcileResponse(BaseModel):
    ok: bool
    requeued: int
    streams: List[str]


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


@router.post("/queue/reconcile", response_model=ReconcileResponse)
async def reconcile_queue(
    req: ReconcileRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    qm: QueueManager = Depends(get_queue_manager),
) -> ReconcileResponse:
    """
    Reconcile Redis Streams pending entries (PEL) for stuck consumers.

    This is the "perfect-perfect" safety net for the crash window:
      XREADGROUP succeeds but worker never claims/leases.
    """
    streams = req.streams
    if not streams:
        stream_key = getattr(qm, "stream_key", "jobs:stream")
        streams = [
            f"{stream_key}:emergency",
            f"{stream_key}:high",
            f"{stream_key}:normal",
            f"{stream_key}:low",
        ]

    idle_ms = int(req.pel_idle_threshold_seconds * 1000)

    async def check_active_lease(job_id: str) -> bool:
        q = select(JobLease).where(JobLease.job_id == job_id, JobLease.expires_at > utcnow())
        res = await session.execute(q)
        return res.scalars().first() is not None

    total = 0
    for s in streams:
        total += await qm.pel_reconcile_requeue(
            stream=s,
            idle_ms=idle_ms,
            check_active_lease=check_active_lease,
            batch_size=req.batch_size,
        )

    logger.info("queue_reconciled", requeued=total, streams=streams)
    return ReconcileResponse(ok=True, requeued=total, streams=streams)

