"""
End-to-end tests for API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, Mock, patch


@pytest.fixture
def mock_orchestrator():
    """Mock orchestrator for API tests."""
    orchestrator = Mock()
    orchestrator.create_job = AsyncMock(return_value="test-job-123")
    orchestrator.get_job_status = AsyncMock(return_value={
        "job_id": "test-job-123",
        "status": "pending",
        "progress": 0.0
    })
    orchestrator.get_queue_stats = AsyncMock(return_value={
        "normal": {"length": 5, "pending": 2},
        "total": 10
    })
    return orchestrator


@pytest.fixture
def client(mock_orchestrator):
    """Test client with mocked dependencies."""
    # Patch the orchestrator dependency
    with patch("src.main.orchestrator", mock_orchestrator):
        from src.main import app
        with TestClient(app) as c:
            yield c


def test_health_endpoint(client):
    """Test health check endpoint."""
    response = client.get("/health")
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "control-plane"


def test_create_job_endpoint(client):
    """Test job creation endpoint."""
    response = client.post(
        "/api/v1/jobs",
        params={
            "domain": "example.com",
            "url": "https://example.com",
            "job_type": "navigate_extract",
            "strategy": "vanilla",
            "priority": 2
        },
        json={"selector": "h1"}
    )
    
    assert response.status_code == 201
    data = response.json()
    assert "job_id" in data
    assert data["status"] == "created"
    assert data["domain"] == "example.com"


def test_create_job_with_idempotency(client):
    """Test job creation with idempotency key."""
    response = client.post(
        "/api/v1/jobs",
        params={
            "domain": "example.com",
            "url": "https://example.com",
            "job_type": "navigate_extract",
            "strategy": "vanilla",
            "priority": 2,
            "idempotency_key": "unique-key-123"
        },
        json={}
    )
    
    assert response.status_code == 201
    data = response.json()
    assert "job_id" in data


def test_get_job_status_endpoint(client, mock_orchestrator):
    """Test getting job status."""
    response = client.get("/api/v1/jobs/test-job-123")
    
    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == "test-job-123"
    assert "status" in data


def test_get_job_status_not_found(client, mock_orchestrator):
    """Test getting status for non-existent job."""
    mock_orchestrator.get_job_status = AsyncMock(return_value=None)
    
    response = client.get("/api/v1/jobs/nonexistent-job")
    
    assert response.status_code == 404


def test_get_queue_stats_endpoint(client):
    """Test getting queue statistics."""
    response = client.get("/api/v1/queue/stats")
    
    assert response.status_code == 200
    data = response.json()
    assert "normal" in data


def test_root_endpoint(client):
    """Test root endpoint."""
    response = client.get("/")
    
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "control-plane"
    assert data["version"] == "1.0.0"
    assert data["status"] == "operational"

