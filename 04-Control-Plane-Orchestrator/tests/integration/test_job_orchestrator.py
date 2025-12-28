"""
Integration tests for JobOrchestrator.
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
from src.control_plane.job_orchestrator import JobOrchestrator
from src.control_plane.models import Job, JobStatus


@pytest.mark.asyncio
async def test_create_job_integration(mock_redis, mock_db_session, mock_database):
    """Test creating a job through orchestrator."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    # Mock the session context manager
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    
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
    mock_redis.xadd.assert_called_once()  # Should enqueue
    mock_db_session.add.assert_called_once()  # Should add to DB
    mock_db_session.commit.assert_called_once()  # Should commit


@pytest.mark.asyncio
async def test_create_job_with_idempotency(mock_redis, mock_db_session, mock_database):
    """Test creating a job with idempotency key."""
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    # Mock existing idempotency key
    mock_redis.get.return_value = "existing-job-123"
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    
    job_id = await orchestrator.create_job(
        domain="example.com",
        url="https://example.com",
        job_type="navigate_extract",
        strategy="vanilla",
        payload={},
        priority=2,
        idempotency_key="unique-key-123"
    )
    
    assert job_id == "existing-job-123"
    mock_redis.get.assert_called()  # Should check idempotency
    mock_db_session.add.assert_not_called()  # Should not create new job


@pytest.mark.asyncio
async def test_get_job_status(mock_redis, mock_db_session, mock_database, sample_job):
    """Test getting job status."""
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


@pytest.mark.asyncio
async def test_cancel_job(mock_redis, mock_db_session, mock_database, sample_job):
    """Test canceling a job."""
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
    
    # Mock queue manager remove
    orchestrator.queue_manager.remove = AsyncMock(return_value=True)
    
    result = await orchestrator.cancel_job("test-job-123")
    
    assert result is True
    assert sample_job.status == JobStatus.CANCELLED
    mock_db_session.commit.assert_called_once()


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
        "normal": {"length": 5, "pending": 2},
        "high": {"length": 2, "pending": 0},
        "emergency": {"length": 0, "pending": 0},
        "low": {"length": 3, "pending": 1},
        "dlq": {"length": 1},
        "delayed": {"count": 2}
    })
    
    stats = await orchestrator.get_queue_stats()
    
    assert stats is not None
    assert "normal" in stats
    assert stats["normal"]["length"] == 5

