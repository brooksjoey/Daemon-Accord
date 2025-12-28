"""
Unit tests for IdempotencyEngine.
"""
import pytest
from unittest.mock import AsyncMock
from src.control_plane.idempotency_engine import IdempotencyEngine


@pytest.mark.asyncio
async def test_store_idempotency_key(mock_redis):
    """Test storing an idempotency key."""
    engine = IdempotencyEngine(mock_redis)
    
    await engine.store("unique-key-123", "job-123")
    
    mock_redis.setex.assert_called_once_with("idempotency:unique-key-123", 86400, "job-123")


@pytest.mark.asyncio
async def test_check_idempotency_key_exists(mock_redis):
    """Test checking for existing idempotency key."""
    engine = IdempotencyEngine(mock_redis)
    mock_redis.get.return_value = "job-123"
    
    result = await engine.check("unique-key-123")
    
    assert result == "job-123"
    mock_redis.get.assert_called_once_with("idempotency:unique-key-123")


@pytest.mark.asyncio
async def test_check_idempotency_key_not_exists(mock_redis):
    """Test checking for non-existent idempotency key."""
    engine = IdempotencyEngine(mock_redis)
    mock_redis.get.return_value = None
    
    result = await engine.check("unique-key-123")
    
    assert result is None


@pytest.mark.asyncio
async def test_delete_idempotency_key(mock_redis):
    """Test deleting an idempotency key."""
    engine = IdempotencyEngine(mock_redis)
    mock_redis.delete = AsyncMock(return_value=1)  # Make it async and return 1 (deleted)
    
    result = await engine.delete("unique-key-123")
    
    assert result is True
    mock_redis.delete.assert_called_once_with("idempotency:unique-key-123")


@pytest.mark.asyncio
async def test_exists_idempotency_key(mock_redis):
    """Test checking if idempotency key exists."""
    engine = IdempotencyEngine(mock_redis)
    mock_redis.exists = AsyncMock(return_value=1)  # Make it async
    
    result = await engine.exists("unique-key-123")
    
    assert result is True
    mock_redis.exists.assert_called_once_with("idempotency:unique-key-123")


@pytest.mark.asyncio
async def test_exists_idempotency_key_not_found(mock_redis):
    """Test checking if idempotency key doesn't exist."""
    engine = IdempotencyEngine(mock_redis)
    mock_redis.exists = AsyncMock(return_value=0)  # Make it async
    
    result = await engine.exists("unique-key-123")
    
    assert result is False
    mock_redis.exists.assert_called_once_with("idempotency:unique-key-123")

