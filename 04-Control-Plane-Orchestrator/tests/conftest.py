"""
Pytest configuration and shared fixtures for all tests.
"""
import pytest
import sys
import os
from unittest.mock import Mock, AsyncMock
from typing import AsyncGenerator

# Add parent directory to path for proper imports
parent_path = os.path.join(os.path.dirname(__file__), "..")
if parent_path not in sys.path:
    sys.path.insert(0, parent_path)

# Clear SQLModel metadata before imports to avoid table redefinition
from sqlmodel import SQLModel
SQLModel.metadata.clear()

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Import using absolute path from src
from src.config import ControlPlaneSettings
from src.database import Database
from src.control_plane.models import Job, JobExecution, JobStatus


@pytest.fixture(autouse=True, scope="function")
def reset_sqlmodel_metadata():
    """
    Reset SQLModel metadata before each test to avoid table redefinition errors.
    This ensures test isolation.
    """
    # Clear metadata before test
    SQLModel.metadata.clear()
    yield
    # Clear metadata after test
    SQLModel.metadata.clear()


@pytest.fixture(autouse=True, scope="session")
def _disable_heavy_startup_for_tests():
    """
    Ensure importing/running FastAPI app in tests does not require live
    Postgres/Redis/Playwright.
    """
    os.environ.setdefault("SKIP_INIT_MODELS", "true")
    os.environ.setdefault("DISABLE_CONTROL_PLANE_STARTUP", "true")
    yield


@pytest.fixture
def settings():
    """Test settings with in-memory/in-memory configurations."""
    return ControlPlaneSettings(
        postgres_dsn="postgresql+asyncpg://test:test@localhost:5432/test_daemon_accord",
        redis_url="redis://localhost:6379/1",  # Use DB 1 for tests
        max_concurrent_jobs=10,
        worker_count=2,
    )


@pytest.fixture
async def mock_redis():
    """Mock Redis client for unit tests."""
    redis_client = AsyncMock(spec=redis.Redis)
    redis_client.get = AsyncMock(return_value=None)
    redis_client.setex = AsyncMock()
    redis_client.delete = AsyncMock(return_value=1)  # Make async
    redis_client.exists = AsyncMock(return_value=0)  # Make async
    redis_client.incr = AsyncMock()
    redis_client.decr = AsyncMock()
    redis_client.xadd = AsyncMock(return_value="msg-123-0")
    redis_client.xreadgroup = AsyncMock(return_value=[])
    redis_client.xack = AsyncMock()
    redis_client.xlen = AsyncMock(return_value=0)
    redis_client.xpending = AsyncMock(return_value=(0, None, None, []))
    redis_client.xpending_range = AsyncMock(return_value=[])
    redis_client.xclaim = AsyncMock(return_value=[])
    redis_client.execute_command = AsyncMock(return_value=[])
    redis_client.zadd = AsyncMock()
    redis_client.zcard = AsyncMock(return_value=0)
    redis_client.xrange = AsyncMock(return_value=[])
    redis_client.xdel = AsyncMock()
    redis_client.xgroup_create = AsyncMock()
    redis_client.aclose = AsyncMock()
    return redis_client


@pytest.fixture
async def mock_db_engine():
    """Mock database engine for unit tests."""
    engine = Mock(spec=AsyncEngine)
    return engine


@pytest.fixture
async def mock_db_session():
    """Mock async database session for unit tests."""
    session = AsyncMock(spec=AsyncSession)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.get = AsyncMock(return_value=None)
    # SQLAlchemy AsyncSession.add() is synchronous
    session.add = Mock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    # SQLAlchemy result objects have synchronous .all()/.scalars()
    scalars_result = Mock()
    scalars_result.all = Mock(return_value=[])
    scalars_result.first = Mock(return_value=None)

    execute_result = Mock()
    execute_result.all = Mock(return_value=[])
    execute_result.scalars = Mock(return_value=scalars_result)

    session.execute = AsyncMock(return_value=execute_result)
    session.exec = AsyncMock(return_value=Mock(all=Mock(return_value=[])))
    return session


@pytest.fixture
async def mock_database(mock_db_engine, mock_db_session):
    """Mock Database instance."""
    db = Mock(spec=Database)
    db.engine = mock_db_engine
    db.session = Mock(return_value=mock_db_session)
    return db


@pytest.fixture
def sample_job_data():
    """Sample job data for testing."""
    return {
        "id": "test-job-123",
        "domain": "example.com",
        "url": "https://example.com",
        "job_type": "navigate_extract",
        "strategy": "vanilla",
        "payload": '{"selector": "h1"}',
        "priority": 2,
        "status": JobStatus.PENDING,
        "max_attempts": 3,
        "attempts": 0,
        "timeout_seconds": 300,
    }


@pytest.fixture
def sample_job(sample_job_data):
    """Sample Job instance for testing."""
    return Job(**sample_job_data)


@pytest.fixture
def sample_job_result():
    """Sample job execution result."""
    return {
        "success": True,
        "data": {"content": "Test content", "title": "Test Title"},
        "artifacts": {"screenshot": "path/to/screenshot.png"},
        "error": None,
        "execution_time": 1.5,
    }

