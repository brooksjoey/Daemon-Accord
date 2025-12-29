import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from src.api.internal_worker import (
    claim_job,
    heartbeat,
    complete,
    fail,
    ClaimRequest,
    HeartbeatRequest,
    CompleteRequest,
    FailRequest,
    FailError,
)
from src.control_plane.models import Job, JobStatus
from src.db.models_worker import JobLease
from src.services.lease_reaper import LeaseReaper
from src.control_plane.queue_manager import QueueManager


class _Txn:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _dummy_request(**settings_kwargs):
    from src.config import ControlPlaneSettings

    settings = ControlPlaneSettings(**settings_kwargs)
    app = SimpleNamespace(state=SimpleNamespace(settings=settings))
    return SimpleNamespace(app=app)


@pytest.mark.asyncio
async def test_claim_acquires_lease_and_sets_running(mock_redis):
    job = Job(
        id="11111111-1111-1111-1111-111111111111",
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload=json.dumps({"selector": "h1"}),
        priority=2,
        status=JobStatus.QUEUED,
        max_attempts=3,
        attempts=0,
        timeout_seconds=300,
    )

    session = Mock()
    session.begin = Mock(return_value=_Txn())
    session.get = AsyncMock(side_effect=lambda model, key, **kw: job if model is Job else None)
    session.add = Mock()
    session.flush = AsyncMock()
    session.delete = AsyncMock()
    session.close = AsyncMock()

    qm = QueueManager(mock_redis)
    qm.read_one_from_any_stream = AsyncMock(return_value=("jobs:stream:normal", "1-0", {"job_id": job.id}))
    qm.ack = AsyncMock()

    req = ClaimRequest(worker_id="w1", streams=["jobs:stream:normal"], max_wait_ms=1)
    resp = await claim_job(req=req, request=_dummy_request(), session=session, qm=qm)

    assert resp.claimed is True
    assert resp.job_id == job.id
    assert job.status == JobStatus.RUNNING
    assert job.attempts == 1
    qm.ack.assert_not_called()


@pytest.mark.asyncio
async def test_heartbeat_extends_lease_expiry():
    lease = JobLease.new(
        job_id="11111111-1111-1111-1111-111111111111",
        worker_id="w1",
        attempt_id=uuid4(),
        ttl_seconds=1,
    )
    old_expires = lease.expires_at

    session = Mock()
    session.begin = Mock(return_value=_Txn())
    session.get = AsyncMock(side_effect=lambda model, key, **kw: lease if model is JobLease else None)
    session.add = Mock()
    session.close = AsyncMock()

    req = HeartbeatRequest(worker_id="w1", job_id=lease.job_id, lease_token=lease.lease_token)
    resp = await heartbeat(req=req, request=_dummy_request(worker_lease_ttl_seconds=60), session=session)

    assert resp.ok is True
    assert lease.expires_at > old_expires


@pytest.mark.asyncio
async def test_complete_is_idempotent_and_releases_lease():
    job = Job(
        id="11111111-1111-1111-1111-111111111111",
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload=json.dumps({"selector": "h1"}),
        priority=2,
        status=JobStatus.RUNNING,
        max_attempts=3,
        attempts=1,
        timeout_seconds=300,
    )
    attempt_id = uuid4()
    lease = JobLease.new(job_id=job.id, worker_id="w1", attempt_id=attempt_id, ttl_seconds=60)

    session = Mock()
    session.begin = Mock(return_value=_Txn())

    async def _get(model, key, **kw):
        if model is JobLease:
            return lease
        if model is Job:
            return job
        return None

    session.get = AsyncMock(side_effect=_get)
    session.delete = AsyncMock()
    session.close = AsyncMock()

    req = CompleteRequest(
        worker_id="w1",
        job_id=job.id,
        attempt_id=attempt_id,
        lease_token=lease.lease_token,
        stream="jobs:stream:normal",
        message_id="1-0",
        result={"success": True, "data": {"ok": 1}},
    )
    resp = await complete(req=req, session=session)

    assert resp.ok is True
    assert resp.ack is True
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None
    session.delete.assert_called()  # lease deleted


@pytest.mark.asyncio
async def test_fail_requeues_until_max_attempts_then_dlq(mock_redis):
    # retry case
    job = Job(
        id="11111111-1111-1111-1111-111111111111",
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload=json.dumps({"selector": "h1"}),
        priority=2,
        status=JobStatus.RUNNING,
        max_attempts=3,
        attempts=1,
        timeout_seconds=300,
    )
    attempt_id = uuid4()
    lease = JobLease.new(job_id=job.id, worker_id="w1", attempt_id=attempt_id, ttl_seconds=60)

    session = Mock()
    session.begin = Mock(return_value=_Txn())

    async def _get_retry(model, key, **kw):
        if model is JobLease:
            return lease
        if model is Job:
            return job
        return None

    session.get = AsyncMock(side_effect=_get_retry)
    session.delete = AsyncMock()
    session.close = AsyncMock()

    qm = QueueManager(mock_redis)
    qm.enqueue = AsyncMock(return_value="2-0")
    qm.dlq_add = AsyncMock(return_value="dlq-1")

    req = FailRequest(
        worker_id="w1",
        job_id=job.id,
        attempt_id=attempt_id,
        lease_token=lease.lease_token,
        stream="jobs:stream:normal",
        message_id="1-0",
        error=FailError(code="ERR", message="boom", retryable=True),
    )
    resp = await fail(req=req, session=session, qm=qm)
    assert resp.ok is True
    assert resp.ack is True
    assert resp.requeued is True
    qm.enqueue.assert_called_once()
    qm.dlq_add.assert_not_called()

    # dlq case
    job2 = Job(**{**job.model_dump(), "id": "22222222-2222-2222-2222-222222222222", "attempts": 3})
    attempt2 = uuid4()
    lease2 = JobLease.new(job_id=job2.id, worker_id="w1", attempt_id=attempt2, ttl_seconds=60)

    async def _get_dlq(model, key, **kw):
        if model is JobLease:
            return lease2
        if model is Job:
            return job2
        return None

    session2 = Mock()
    session2.begin = Mock(return_value=_Txn())
    session2.get = AsyncMock(side_effect=_get_dlq)
    session2.delete = AsyncMock()
    session2.close = AsyncMock()

    qm2 = QueueManager(mock_redis)
    qm2.enqueue = AsyncMock()
    qm2.dlq_add = AsyncMock(return_value="dlq-2")

    req2 = FailRequest(
        worker_id="w1",
        job_id=job2.id,
        attempt_id=attempt2,
        lease_token=lease2.lease_token,
        stream="jobs:stream:normal",
        message_id="9-0",
        error=FailError(code="ERR", message="boom", retryable=True),
    )
    resp2 = await fail(req=req2, session=session2, qm=qm2)
    assert resp2.ok is True
    assert resp2.ack is True
    assert resp2.dlq is True
    qm2.dlq_add.assert_called_once()


@pytest.mark.asyncio
async def test_lease_reaper_requeues_expired_leases(mock_redis):
    qm = QueueManager(mock_redis)
    qm.enqueue = AsyncMock(return_value="3-0")
    qm.dlq_add = AsyncMock(return_value="dlq-3")
    reaper = LeaseReaper(qm)

    lease = JobLease.new(job_id="33333333-3333-3333-3333-333333333333", worker_id="w1", attempt_id=uuid4(), ttl_seconds=0)

    job = Job(
        id=lease.job_id,
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload=json.dumps({}),
        priority=1,
        status=JobStatus.RUNNING,
        max_attempts=3,
        attempts=1,
        timeout_seconds=300,
    )

    session = Mock()
    # session.execute(select(JobLease)...).scalars().all() -> [lease]
    exec_result = Mock()
    exec_result.scalars.return_value.all.return_value = [lease]
    session.execute = AsyncMock(return_value=exec_result)
    session.begin = Mock(return_value=_Txn())
    session.get = AsyncMock(return_value=job)
    session.delete = AsyncMock()

    recovered = await reaper.run_once(session)
    assert recovered == 1
    qm.enqueue.assert_called_once()


@pytest.mark.asyncio
async def test_pel_reconcile_requeues_only_without_active_lease(mock_redis):
    qm = QueueManager(mock_redis)

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0", "consumer": "dead", "time_since_delivered": 120000, "times_delivered": 1},
    ]
    mock_redis.xrange.return_value = [("1-0", {"job_id": "44444444-4444-4444-4444-444444444444"})]

    async def no_lease(_job_id: str) -> bool:
        return False

    count = await qm.pel_reconcile_requeue(
        stream="jobs:stream:normal",
        idle_ms=60000,
        check_active_lease=no_lease,
        batch_size=100,
    )
    assert count == 1
    mock_redis.xclaim.assert_called_once()
    mock_redis.xadd.assert_called()
    mock_redis.xack.assert_called_once()

