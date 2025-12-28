"""
Unit tests for QueueManager.
"""
import pytest
from unittest.mock import AsyncMock, Mock
from src.control_plane.queue_manager import QueueManager


@pytest.mark.asyncio
async def test_enqueue_job(mock_redis):
    """Test enqueueing a job."""
    manager = QueueManager(mock_redis)
    
    job_id = "test-job-123"
    priority = 2
    domain = "example.com"
    
    message_id = await manager.enqueue(job_id, priority, domain)
    
    assert message_id == "msg-123-0"
    mock_redis.xadd.assert_called_once()
    call_args = mock_redis.xadd.call_args
    assert call_args[0][0] == "jobs:stream:normal"  # Priority 2 = normal


@pytest.mark.asyncio
async def test_enqueue_high_priority(mock_redis):
    """Test enqueueing high priority job."""
    manager = QueueManager(mock_redis)
    
    job_id = "test-job-456"
    priority = 1  # High priority
    domain = "example.com"
    
    await manager.enqueue(job_id, priority, domain)
    
    call_args = mock_redis.xadd.call_args
    assert call_args[0][0] == "jobs:stream:high"


@pytest.mark.asyncio
async def test_enqueue_emergency_priority(mock_redis):
    """Test enqueueing emergency priority job."""
    manager = QueueManager(mock_redis)
    
    job_id = "test-job-789"
    priority = 0  # Emergency
    domain = "example.com"
    
    await manager.enqueue(job_id, priority, domain)
    
    call_args = mock_redis.xadd.call_args
    assert call_args[0][0] == "jobs:stream:emergency"


@pytest.mark.asyncio
async def test_enqueue_with_deduplication(mock_redis):
    """Test enqueueing with deduplication key."""
    manager = QueueManager(mock_redis)
    mock_redis.get.return_value = "existing-job-123"
    
    job_id = "test-job-123"
    priority = 2
    domain = "example.com"
    dedupe_key = "unique-key-123"
    
    result = await manager.enqueue(job_id, priority, domain, dedupe_key=dedupe_key)
    
    assert result == "existing-job-123"
    mock_redis.get.assert_called_once_with(f"dedupe:{dedupe_key}")
    mock_redis.xadd.assert_not_called()  # Should not enqueue if duplicate


@pytest.mark.asyncio
async def test_dequeue_job(mock_redis):
    """Test dequeuing a job."""
    manager = QueueManager(mock_redis)
    manager.consumer_name = "test-worker"
    
    # Mock response with job
    mock_redis.xreadgroup.return_value = [
        (b"jobs:stream:normal", [(b"123-0", {b"job_id": b"test-job-123"})])
    ]
    
    job_id = await manager.dequeue(timeout=1.0)
    
    assert job_id == "test-job-123"
    mock_redis.xreadgroup.assert_called()
    mock_redis.xack.assert_called_once()


@pytest.mark.asyncio
async def test_dequeue_no_jobs(mock_redis):
    """Test dequeuing when no jobs available."""
    manager = QueueManager(mock_redis)
    manager.consumer_name = "test-worker"
    
    mock_redis.xreadgroup.return_value = []
    
    job_id = await manager.dequeue(timeout=0.1)
    
    assert job_id is None


@pytest.mark.asyncio
async def test_requeue_job(mock_redis):
    """Test requeueing a job."""
    manager = QueueManager(mock_redis)
    
    job_id = "test-job-123"
    priority = 2
    domain = "example.com"
    
    result = await manager.requeue(job_id, priority, domain, delay_seconds=0)
    
    assert result == "msg-123-0"
    mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_requeue_with_delay(mock_redis):
    """Test requeueing a job with delay."""
    manager = QueueManager(mock_redis)
    
    job_id = "test-job-123"
    priority = 2
    domain = "example.com"
    
    result = await manager.requeue(job_id, priority, domain, delay_seconds=60)
    
    assert result == f"delayed:{job_id}"
    mock_redis.zadd.assert_called_once()


@pytest.mark.asyncio
async def test_get_stats(mock_redis):
    """Test getting queue statistics."""
    manager = QueueManager(mock_redis)
    mock_redis.xlen.return_value = 5
    mock_redis.xpending.return_value = (2, None, None, [])
    
    stats = await manager.get_stats()
    
    assert "emergency" in stats
    assert "high" in stats
    assert "normal" in stats
    assert "low" in stats
    assert "dlq" in stats
    assert "delayed" in stats
    assert stats["normal"]["length"] == 5


@pytest.mark.asyncio
async def test_get_depth(mock_redis):
    """Test getting total queue depth."""
    manager = QueueManager(mock_redis)
    mock_redis.xlen.return_value = 3
    
    depth = await manager.get_depth()
    
    assert depth == 12  # 4 streams * 3 jobs each
    assert mock_redis.xlen.call_count == 4


@pytest.mark.asyncio
async def test_remove_job(mock_redis):
    """Test removing a job from queue."""
    manager = QueueManager(mock_redis)
    
    # Mock finding the job in stream
    mock_redis.xrange.return_value = [
        (b"123-0", {b"job_id": b"test-job-123"})
    ]
    
    result = await manager.remove("test-job-123")
    
    assert result is True
    mock_redis.xdel.assert_called_once()


@pytest.mark.asyncio
async def test_initialize_consumer_group(mock_redis):
    """Test initializing consumer group."""
    manager = QueueManager(mock_redis)
    manager.consumer_name = "test-worker"
    
    await manager.initialize_consumer_group("test-worker")
    
    assert mock_redis.xgroup_create.call_count == 4  # One for each priority stream

