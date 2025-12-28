"""
Unit tests for ExecutorAdapter.

Tests the adapter that bridges Control Plane with Execution Engine.
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from src.control_plane.executor_adapter import ExecutorAdapter


@pytest.mark.asyncio
async def test_executor_adapter_initialization(mock_redis, mock_db_session):
    """Test ExecutorAdapter initialization."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    assert adapter.redis_client == mock_redis
    assert adapter.db_session == mock_db_session
    assert adapter.browser_pool is None


@pytest.mark.asyncio
async def test_get_executor_vanilla_strategy(mock_redis, mock_db_session, sample_job):
    """Test getting vanilla executor for vanilla strategy."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    sample_job.strategy = "vanilla"
    
    executor = adapter._get_executor(sample_job)
    
    assert executor is not None
    # Should return VanillaExecutor or mock if Execution Engine not available


@pytest.mark.asyncio
async def test_get_executor_stealth_strategy(mock_redis, mock_db_session, sample_job):
    """Test getting stealth executor for stealth strategy."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    sample_job.strategy = "stealth"
    
    executor = adapter._get_executor(sample_job)
    
    assert executor is not None


@pytest.mark.asyncio
async def test_get_executor_assault_strategy(mock_redis, mock_db_session, sample_job):
    """Test getting assault executor for assault strategy."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    sample_job.strategy = "assault"
    
    executor = adapter._get_executor(sample_job)
    
    assert executor is not None


@pytest.mark.asyncio
async def test_execute_job_success(mock_redis, mock_db_session):
    """Test executing a job successfully."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    # Mock executor
    mock_executor = AsyncMock()
    mock_executor.execute = AsyncMock(return_value=Mock(
        success=True,
        data={"content": "Test"},
        artifacts={},
        error=None
    ))
    
    adapter._get_executor = Mock(return_value=mock_executor)
    
    job = Mock()
    job.id = "test-job-123"
    job.url = "https://example.com"
    job.type = "navigate_extract"
    job.strategy = "vanilla"
    job.payload = {}
    
    result = await adapter.execute(job)
    
    assert result["success"] is True
    assert "data" in result
    mock_executor.execute.assert_called_once_with(job)


@pytest.mark.asyncio
async def test_execute_job_failure(mock_redis, mock_db_session):
    """Test executing a job that fails."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    # Mock executor that fails
    mock_executor = AsyncMock()
    mock_executor.execute = AsyncMock(return_value=Mock(
        success=False,
        data={},
        artifacts={},
        error="Execution failed"
    ))
    
    adapter._get_executor = Mock(return_value=mock_executor)
    
    job = Mock()
    job.id = "test-job-123"
    job.url = "https://example.com"
    job.type = "navigate_extract"
    job.strategy = "vanilla"
    job.payload = {}
    
    result = await adapter.execute(job)
    
    assert result["success"] is False
    assert result["error"] == "Execution failed"


@pytest.mark.asyncio
async def test_execute_job_execution_engine_unavailable(mock_redis, mock_db_session):
    """Test executing when Execution Engine is not available."""
    adapter = ExecutorAdapter(
        redis_client=mock_redis,
        db_session=mock_db_session,
        browser_pool=None
    )
    
    # Mock that Execution Engine is not available
    adapter._get_executor = Mock(side_effect=ImportError("Execution Engine not available"))
    
    job = Mock()
    job.id = "test-job-123"
    job.url = "https://example.com"
    job.type = "navigate_extract"
    job.strategy = "vanilla"
    job.payload = {}
    
    result = await adapter.execute(job)
    
    # Should return failure result when Execution Engine unavailable
    assert result["success"] is False
    assert "error" in result

