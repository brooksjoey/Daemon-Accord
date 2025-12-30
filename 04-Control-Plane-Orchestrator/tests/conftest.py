import pytest
import asyncio
from typing import Dict, Any
from fastapi.testclient import TestClient

from main import app
from storage.redis_client import RedisClient
from database.queries import JobQueries
import asyncpg

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def test_db():
    """Test database connection"""
    conn = await asyncpg.connect(
        "postgresql://test:test@localhost/test_db"
    )
    yield conn
    await conn.close()

@pytest.fixture
async def redis_client():
    """Test Redis client"""
    client = RedisClient("redis://localhost:6379/1")
    await client.connect()
    yield client
    await client.flush_db()
    await client.disconnect()

@pytest.fixture
def test_client():
    """Test FastAPI client"""
    return TestClient(app)

@pytest.fixture
def sample_workflow() -> Dict[str, Any]:
    """Sample workflow for testing"""
    return {
        "name": "test_workflow",
        "version": "1.0.0",
        "steps": [
            {
                "type": "navigate",
                "url": "https://example.com",
                "wait_for": [".header"],
                "capture": True
            },
            {
                "type": "extract",
                "selector": "h1",
                "attribute": "textContent"
            }
        ],
        "timeout": 60,
        "priority": 1,
        "capture_artifacts": True
    }

