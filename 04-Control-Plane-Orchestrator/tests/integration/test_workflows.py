"""
Integration tests for Workflow system.

Tests workflow execution against real API endpoints.
"""
import pytest
import httpx
from src.workflows.workflow_registry import get_workflow_registry


@pytest.mark.asyncio
@pytest.mark.integration
async def test_list_workflows_endpoint():
    """Test listing workflows via API."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get("http://localhost:8082/api/v1/workflows")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "page_change_detection" in data
        assert "job_posting_monitor" in data
        assert "uptime_smoke_check" in data


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_workflow_details():
    """Test getting workflow details via API."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get("http://localhost:8082/api/v1/workflows/page_change_detection")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["name"] == "page_change_detection"
        assert "input_schema" in data
        assert "output_schema" in data
        assert "execution_steps" in data


@pytest.mark.asyncio
@pytest.mark.integration
async def test_get_workflow_not_found():
    """Test getting non-existent workflow returns 404."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get("http://localhost:8082/api/v1/workflows/nonexistent")
        
        assert response.status_code == 404


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_page_change_detection_workflow():
    """Test running page change detection workflow."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Submit workflow
        response = await client.post(
            "http://localhost:8082/api/v1/workflows/page_change_detection/run",
            json={
                "url": "https://example.com",
                "domain": "example.com",
                "selectors": ["h1"]
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        
        assert "job_id" in data
        assert data["workflow_name"] == "page_change_detection"
        assert data["status"] == "pending"
        
        # Verify job was created
        job_id = data["job_id"]
        status_response = await client.get(f"http://localhost:8082/api/v1/jobs/{job_id}")
        assert status_response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_job_posting_monitor_workflow():
    """Test running job posting monitor workflow."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "http://localhost:8082/api/v1/workflows/job_posting_monitor/run",
            json={
                "url": "https://jsonplaceholder.typicode.com/",
                "domain": "jsonplaceholder.typicode.com",
                "extract_fields": {
                    "title": "h1",
                    "content": "p"
                }
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        
        assert "job_id" in data
        assert data["workflow_name"] == "job_posting_monitor"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_uptime_smoke_check_workflow():
    """Test running uptime smoke check workflow."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "http://localhost:8082/api/v1/workflows/uptime_smoke_check/run",
            json={
                "url": "https://example.com",
                "domain": "example.com",
                "required_selectors": ["h1", "body"],
                "screenshot": True
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        
        assert "job_id" in data
        assert data["workflow_name"] == "uptime_smoke_check"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_workflow_invalid_input():
    """Test running workflow with invalid input returns 400."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "http://localhost:8082/api/v1/workflows/page_change_detection/run",
            json={
                "url": "https://example.com"
                # Missing required fields: domain, selectors
            }
        )
        
        assert response.status_code == 400


@pytest.mark.asyncio
@pytest.mark.integration
async def test_run_workflow_with_webhook():
    """Test running workflow with webhook URL."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "http://localhost:8082/api/v1/workflows/page_change_detection/run",
            json={
                "url": "https://example.com",
                "domain": "example.com",
                "selectors": ["h1"],
                "webhook_url": "https://httpbin.org/post"  # Test webhook endpoint
            }
        )
        
        assert response.status_code == 201
        data = response.json()
        assert "job_id" in data

