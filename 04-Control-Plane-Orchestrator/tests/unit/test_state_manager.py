"""
Unit tests for StateManager.
"""
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, Mock
from src.control_plane.state_manager import StateManager
from src.control_plane.models import Job, JobStatus


@pytest.mark.asyncio
async def test_get_job_state_from_cache(mock_redis, mock_db_engine):
    """Test getting job state from cache."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    cached_state = '{"id": "job-123", "status": "pending"}'
    mock_redis.get.return_value = cached_state
    
    state = await manager.get_job_state("job-123")
    
    assert state is not None
    assert state["id"] == "job-123"
    mock_redis.get.assert_called_once_with("job:state:job-123")


@pytest.mark.asyncio
async def test_get_job_state_from_db(mock_redis, mock_db_engine, mock_db_session, sample_job):
    """Test getting job state from database when not in cache."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    mock_redis.get.return_value = None  # Not in cache
    mock_db_session.get.return_value = sample_job
    sample_job.model_dump_json = Mock(return_value='{"id": "test-job-123", "status": "pending"}')
    
    state = await manager.get_job_state("test-job-123")
    
    assert state is not None
    mock_db_session.get.assert_called_once()
    mock_redis.setex.assert_called_once()  # Should cache the result


@pytest.mark.asyncio
async def test_get_job_state_not_found(mock_redis, mock_db_engine, mock_db_session):
    """Test getting job state when job doesn't exist."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    mock_redis.get.return_value = None
    mock_db_session.get.return_value = None
    
    state = await manager.get_job_state("nonexistent-job")
    
    assert state is None


@pytest.mark.asyncio
async def test_update_job_status_to_running(mock_redis, mock_db_engine, mock_db_session, sample_job):
    """Test updating job status to running."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    mock_db_session.get.return_value = sample_job
    
    result = await manager.update_job_status(
        "test-job-123",
        JobStatus.RUNNING
    )
    
    assert result is True
    assert sample_job.status == JobStatus.RUNNING
    assert sample_job.started_at is not None
    mock_db_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_update_job_status_to_completed(mock_redis, mock_db_engine, mock_db_session, sample_job, sample_job_result):
    """Test updating job status to completed."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    mock_db_session.get.return_value = sample_job
    
    result = await manager.update_job_status(
        "test-job-123",
        JobStatus.COMPLETED,
        result=sample_job_result
    )
    
    assert result is True
    assert sample_job.status == JobStatus.COMPLETED
    assert sample_job.completed_at is not None
    assert sample_job.result is not None


@pytest.mark.asyncio
async def test_update_job_status_with_error(mock_redis, mock_db_engine, mock_db_session, sample_job):
    """Test updating job status with error."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    mock_db_session.get.return_value = sample_job
    
    error_msg = "Test error message"
    result = await manager.update_job_status(
        "test-job-123",
        JobStatus.FAILED,
        error=error_msg
    )
    
    assert result is True
    assert sample_job.status == JobStatus.FAILED
    assert sample_job.error == error_msg
    assert sample_job.completed_at is not None


@pytest.mark.asyncio
async def test_increment_attempts(mock_redis, mock_db_engine, mock_db_session, sample_job):
    """Test incrementing job attempts."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    mock_db_session.get.return_value = sample_job
    initial_attempts = sample_job.attempts
    
    result = await manager.increment_attempts("test-job-123")
    
    assert result == initial_attempts + 1
    assert sample_job.attempts == initial_attempts + 1
    mock_db_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_get_jobs_by_status(mock_redis, mock_db_engine, mock_db_session, sample_job):
    """Test getting jobs by status."""
    manager = StateManager(mock_redis, mock_db_engine)
    
    from sqlalchemy.engine import Result
    mock_result = Mock(spec=Result)
    mock_result.scalars.return_value.all.return_value = [sample_job]
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    mock_db_session.execute = AsyncMock(return_value=mock_result)
    
    jobs = await manager.get_jobs_by_status(JobStatus.PENDING)
    
    assert len(jobs) == 1
    assert jobs[0].id == "test-job-123"
    mock_db_session.execute.assert_called_once()

