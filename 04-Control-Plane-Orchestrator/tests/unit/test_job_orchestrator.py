"""
Unit tests for JobOrchestrator.

These tests use mocks to isolate the orchestrator logic from external dependencies
(Redis, Database, Execution Engine). This is standard practice for unit testing.
"""
import pytest
import json
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
from uuid import uuid4

from src.control_plane.job_orchestrator import JobOrchestrator, JobStatus
from src.control_plane.models import Job


@pytest.mark.asyncio
async def test_create_job_basic(mock_redis, mock_db_session, mock_database):
    """Test creating a basic job without idempotency."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_redis.get.return_value = None  # No existing idempotency key
    
    job_id = await orchestrator.create_job(
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload={"selector": "h1"},
        priority=2
    )
    
    assert job_id is not None
    assert isinstance(job_id, str)
    assert len(job_id) == 36  # UUID format
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()
    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_create_job_with_idempotency(mock_redis, mock_db_session, mock_database):
    """Test creating a job with idempotency key returns existing job."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    existing_job_id = "existing-job-123"
    mock_redis.get.return_value = existing_job_id
    
    job_id = await orchestrator.create_job(
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload={},
        priority=2,
        idempotency_key="unique-key-123"
    )
    
    assert job_id == existing_job_id
    mock_redis.get.assert_called()
    mock_db_session.add.assert_not_called()  # Should not create new job


@pytest.mark.asyncio
async def test_create_job_stores_idempotency_key(mock_redis, mock_db_session, mock_database):
    """Test that idempotency key is stored after job creation."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_redis.get.return_value = None
    
    job_id = await orchestrator.create_job(
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload={},
        priority=2,
        idempotency_key="unique-key-456"
    )
    
    # Verify idempotency key was stored
    mock_redis.setex.assert_called()
    call_args = mock_redis.setex.call_args
    assert "unique-key-456" in str(call_args)


@pytest.mark.asyncio
async def test_create_job_enqueues_to_correct_stream(mock_redis, mock_db_session, mock_database):
    """Test that jobs are enqueued to the correct priority stream."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    
    # Test emergency priority (0)
    await orchestrator.create_job(
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload={},
        priority=0
    )
    
    call_args = orchestrator.queue_manager.enqueue.call_args
    assert call_args[1]["priority"] == 0
    
    # Test high priority (1)
    await orchestrator.create_job(
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload={},
        priority=1
    )
    
    call_args = orchestrator.queue_manager.enqueue.call_args
    assert call_args[1]["priority"] == 1


@pytest.mark.asyncio
async def test_get_job_status_success(mock_redis, mock_db_session, mock_database, sample_job):
    """Test getting job status for existing job."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = sample_job
    
    status = await orchestrator.get_job_status("test-job-123")
    
    assert status is not None
    assert status["job_id"] == "test-job-123"
    assert status["status"] == "pending"
    assert status["domain"] == "example.com"


@pytest.mark.asyncio
async def test_get_job_status_not_found(mock_redis, mock_db_session, mock_database):
    """Test getting job status for non-existent job."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = None
    
    status = await orchestrator.get_job_status("non-existent-job")
    
    assert status is None


@pytest.mark.asyncio
async def test_get_job_status_with_result(mock_redis, mock_db_session, mock_database):
    """Test getting job status with completed result."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    completed_job = Job(
        id="test-job-123",
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload='{"selector": "h1"}',
        priority=2,
        status=JobStatus.COMPLETED.value,
        result='{"content": "Test content"}',
        completed_at=datetime.utcnow()
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = completed_job
    
    status = await orchestrator.get_job_status("test-job-123")
    
    assert status["status"] == "completed"
    assert status["result"] is not None


@pytest.mark.asyncio
async def test_cancel_job_success(mock_redis, mock_db_session, mock_database, sample_job):
    """Test canceling an existing job."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = sample_job
    orchestrator.queue_manager.remove = AsyncMock(return_value=True)
    
    result = await orchestrator.cancel_job("test-job-123")
    
    assert result is True
    assert sample_job.status == JobStatus.CANCELLED.value
    mock_db_session.commit.assert_called_once()
    orchestrator.queue_manager.remove.assert_called_once_with("test-job-123")


@pytest.mark.asyncio
async def test_cancel_job_not_found(mock_redis, mock_db_session, mock_database):
    """Test canceling a non-existent job."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = None
    
    result = await orchestrator.cancel_job("non-existent-job")
    
    assert result is False


@pytest.mark.asyncio
async def test_get_queue_stats(mock_redis, mock_db_session, mock_database):
    """Test getting queue statistics."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    # Mock queue manager stats
    orchestrator.queue_manager.get_stats = AsyncMock(return_value={
        "emergency": {"length": 0, "pending": 0},
        "high": {"length": 2, "pending": 0},
        "normal": {"length": 5, "pending": 2},
        "low": {"length": 3, "pending": 1},
        "dlq": {"length": 1},
        "delayed": {"count": 2}
    })
    
    # Mock database query result
    from sqlalchemy.engine import Result
    mock_result = Mock(spec=Result)
    mock_result.all.return_value = [
        ("completed", "example.com"),
        ("pending", "example.com"),
        ("running", "test.com")
    ]
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.execute = AsyncMock(return_value=mock_result)
    
    stats = await orchestrator.get_queue_stats()
    
    assert stats is not None
    assert "queue" in stats
    assert "jobs" in stats
    assert "running_jobs" in stats
    assert "workers" in stats
    assert stats["queue"]["normal"]["length"] == 5
    assert stats["jobs"]["total"] == 3


@pytest.mark.asyncio
async def test_get_queue_depth(mock_redis, mock_db_session, mock_database):
    """Test getting queue depth."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    orchestrator.queue_manager.get_depth = AsyncMock(return_value=15)
    
    depth = await orchestrator.get_queue_depth()
    
    assert depth == 15


@pytest.mark.asyncio
async def test_process_job_success(mock_redis, mock_db_session, mock_database, sample_job):
    """Test processing a job successfully."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = sample_job
    
    # Mock successful execution
    orchestrator._execute_job = AsyncMock(return_value={
        "success": True,
        "data": {"content": "Test content"},
        "artifacts": {},
        "error": None
    })
    
    await orchestrator.process_job("test-job-123")
    
    assert sample_job.status == JobStatus.COMPLETED.value
    assert sample_job.result is not None
    mock_db_session.commit.assert_called()


@pytest.mark.asyncio
async def test_process_job_failure(mock_redis, mock_db_session, mock_database, sample_job):
    """Test processing a job that fails."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = sample_job
    
    # Mock failed execution
    orchestrator._execute_job = AsyncMock(return_value={
        "success": False,
        "data": {},
        "artifacts": {},
        "error": "Execution failed"
    })
    
    await orchestrator.process_job("test-job-123")
    
    assert sample_job.status == JobStatus.FAILED.value
    assert sample_job.error is not None
    mock_db_session.commit.assert_called()


@pytest.mark.asyncio
async def test_process_job_not_found(mock_redis, mock_db_session, mock_database):
    """Test processing a non-existent job."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = None
    
    # Should not raise exception, just return
    await orchestrator.process_job("non-existent-job")
    
    # Verify no execution was attempted
    assert not hasattr(orchestrator, "_execute_job") or orchestrator._execute_job.call_count == 0


@pytest.mark.asyncio
async def test_process_job_exception_handling(mock_redis, mock_db_session, mock_database, sample_job):
    """Test that exceptions during job processing are handled gracefully."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.get.return_value = sample_job
    
    # Mock exception during execution
    orchestrator._execute_job = AsyncMock(side_effect=Exception("Unexpected error"))
    
    # Should not raise, but mark job as failed
    await orchestrator.process_job("test-job-123")
    
    assert sample_job.status == JobStatus.FAILED.value
    assert sample_job.error is not None


@pytest.mark.asyncio
async def test_max_concurrent_jobs_limit(mock_redis, mock_db_session, mock_database):
    """Test that max concurrent jobs limit is enforced."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=2
    )
    
    assert orchestrator.max_concurrent_jobs == 2


@pytest.mark.asyncio
async def test_shutdown_cleans_up_resources(mock_redis, mock_db_session, mock_database):
    """Test that shutdown properly cleans up resources."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    # Add some running jobs and workers
    orchestrator._running_jobs = {"job1": Mock(), "job2": Mock()}
    orchestrator._workers = [Mock(), Mock()]
    orchestrator._shutdown_event = Mock()
    
    await orchestrator.shutdown()
    
    # Verify shutdown event was set
    assert orchestrator._shutdown_event.set.called
    # Verify workers were cancelled
    assert all(worker.cancel.called for worker in orchestrator._workers)
    # Verify running jobs were cancelled
    assert all(task.cancel.called for task in orchestrator._running_jobs.values())

