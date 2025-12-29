from __future__ import annotations

from datetime import datetime
from typing import Optional

import structlog
from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..control_plane.models import Job, JobStatus
from ..control_plane.queue_manager import QueueManager
from ..db.models_worker import JobLease, utcnow

logger = structlog.get_logger(__name__)


def _utcnow_naive() -> datetime:
    return datetime.utcnow()


class LeaseReaper:
    """
    Periodic task:
      - find expired leases
      - atomically mark job queued (or failed) based on attempts/max_attempts
      - delete lease
      - enqueue new stream message or DLQ

    NOTE: Transport actions happen after DB commit. If transport fails, we do not
    "pretend success" — the pending message/PEL reconciliation can recover.
    """

    def __init__(self, qm: QueueManager) -> None:
        self.qm = qm

    async def run_once(self, session: AsyncSession) -> int:
        # Lock leases to avoid double-reaping when multiple reapers run.
        stmt = (
            select(JobLease)
            .where(JobLease.expires_at <= utcnow())
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        leases = list(result.scalars().all())
        if not leases:
            return 0

        recovered = 0
        for lease in leases:
            job: Optional[Job] = None
            should_retry = False
            try:
                async with session.begin():
                    job = await session.get(Job, lease.job_id, with_for_update=True)
                    if not job:
                        await session.delete(lease)
                        continue

                    attempts = int(job.attempts or 0)
                    max_attempts = int(job.max_attempts or 0)
                    should_retry = attempts < max_attempts

                    if should_retry:
                        job.status = JobStatus.QUEUED
                        job.error = "lease_expired"
                        if hasattr(job, "updated_at"):
                            setattr(job, "updated_at", _utcnow_naive())
                    else:
                        job.status = JobStatus.FAILED
                        job.error = "lease_expired_max_attempts"
                        job.completed_at = _utcnow_naive()
                        if hasattr(job, "updated_at"):
                            setattr(job, "updated_at", _utcnow_naive())

                    await session.delete(lease)

                # Transport actions AFTER commit:
                if should_retry and job is not None:
                    await self.qm.enqueue(
                        job_id=str(job.id),
                        priority=int(job.priority),
                        domain=str(job.domain),
                        job_data={
                            "id": str(job.id),
                            "domain": job.domain,
                            "url": job.url,
                            "type": job.job_type,
                            "strategy": job.strategy,
                            "payload": job.payload,
                            "priority": job.priority,
                            "timeout_seconds": job.timeout_seconds,
                        },
                    )
                elif job is not None:
                    await self.qm.dlq_add({"job_id": str(job.id), "reason": "lease_expired_max_attempts"})

                recovered += 1
            except Exception as e:
                logger.error(
                    "lease_reaper_error",
                    job_id=str(getattr(job, "id", lease.job_id)),
                    error=str(e),
                    exc_info=True,
                )
                # Best-effort: continue with other leases.
                continue

        return recovered

