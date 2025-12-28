"""
Unit tests for Workflow system.

Tests workflow registry, executor, and schema validation.
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
import sys
import os
# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.workflows.workflow_registry import WorkflowRegistry, get_workflow_registry
from src.workflows.workflow_executor import WorkflowExecutor
from src.workflows.models import WorkflowDefinition, WorkflowStatus


def test_workflow_registry_initialization():
    """Test workflow registry initializes with default workflows."""
    registry = WorkflowRegistry()
    
    workflows = registry.list_all()
    
    assert "page_change_detection" in workflows
    assert "job_posting_monitor" in workflows
    assert "uptime_smoke_check" in workflows
    assert len(workflows) == 3


def test_workflow_registry_get():
    """Test getting a workflow by name."""
    registry = WorkflowRegistry()
    
    workflow = registry.get("page_change_detection")
    
    assert workflow is not None
    assert workflow.name == "page_change_detection"
    assert workflow.display_name == "Page Change Detection"


def test_workflow_registry_get_not_found():
    """Test getting non-existent workflow."""
    registry = WorkflowRegistry()
    
    workflow = registry.get("nonexistent")
    
    assert workflow is None


def test_workflow_registry_get_summary():
    """Test getting workflow summary."""
    registry = WorkflowRegistry()
    
    summary = registry.get_summary()
    
    assert "page_change_detection" in summary
    assert "name" in summary["page_change_detection"]
    assert "display_name" in summary["page_change_detection"]
    assert "input_schema" in summary["page_change_detection"]
    assert "output_schema" in summary["page_change_detection"]


def test_workflow_registry_register_custom():
    """Test registering a custom workflow."""
    registry = WorkflowRegistry()
    
    custom_workflow = WorkflowDefinition(
        name="custom_workflow",
        display_name="Custom Workflow",
        description="Test workflow",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        execution_steps=["Step 1"],
        job_type="navigate_extract"
    )
    
    registry.register(custom_workflow)
    
    assert registry.get("custom_workflow") == custom_workflow


@pytest.mark.asyncio
async def test_workflow_executor_initialization(mock_redis, mock_db_session, mock_database):
    """Test workflow executor initialization."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    executor = WorkflowExecutor(orchestrator)
    
    assert executor.job_orchestrator == orchestrator
    assert executor.registry is not None


@pytest.mark.asyncio
async def test_workflow_executor_execute_page_change_detection(mock_redis, mock_db_session, mock_database):
    """Test executing page change detection workflow."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    mock_db_session.__aenter__ = AsyncMock(return_value=mock_db_session)
    mock_db_session.__aexit__ = AsyncMock(return_value=None)
    orchestrator.create_job = AsyncMock(return_value="test-job-123")
    
    executor = WorkflowExecutor(orchestrator)
    
    input_data = {
        "url": "https://example.com",
        "domain": "example.com",
        "selectors": ["h1", "p"]
    }
    
    result = await executor.execute_workflow(
        workflow_name="page_change_detection",
        input_data=input_data
    )
    
    assert result.workflow_name == "page_change_detection"
    assert result.job_id == "test-job-123"
    assert result.status == WorkflowStatus.PENDING
    orchestrator.create_job.assert_called_once()


@pytest.mark.asyncio
async def test_workflow_executor_execute_invalid_workflow(mock_redis, mock_db_session, mock_database):
    """Test executing invalid workflow raises error."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = JobOrchestrator(
        redis_client=mock_redis,
        db=mock_database,
        browser_pool=None,
        db_session=mock_db_session,
        max_concurrent_jobs=10
    )
    
    executor = WorkflowExecutor(orchestrator)
    
    with pytest.raises(ValueError, match="not found"):
        await executor.execute_workflow(
            workflow_name="nonexistent",
            input_data={"url": "https://example.com", "domain": "example.com"}
        )


@pytest.mark.asyncio
async def test_workflow_executor_validate_input_missing_required():
    """Test input validation catches missing required fields."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    workflow = executor.registry.get("page_change_detection")
    
    with pytest.raises(ValueError, match="Missing required field"):
        executor._validate_input(workflow, {"url": "https://example.com"})


@pytest.mark.asyncio
async def test_workflow_executor_convert_to_job_payload_page_change():
    """Test converting page change detection input to job payload."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    workflow = executor.registry.get("page_change_detection")
    input_data = {
        "url": "https://example.com",
        "domain": "example.com",
        "selectors": ["h1", "p"],
        "baseline_content": "abc123",
        "alert_on_change": True
    }
    
    payload = executor._convert_to_job_payload(workflow, input_data)
    
    assert payload["workflow_type"] == "page_change_detection"
    assert payload["baseline_content"] == "abc123"
    assert payload["alert_on_change"] is True
    assert "h1" in payload["selector"]


@pytest.mark.asyncio
async def test_workflow_executor_convert_to_job_payload_job_monitor():
    """Test converting job posting monitor input to job payload."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    workflow = executor.registry.get("job_posting_monitor")
    input_data = {
        "url": "https://jobs.example.com",
        "domain": "jobs.example.com",
        "extract_fields": {"title": "h2", "company": ".company"}
    }
    
    payload = executor._convert_to_job_payload(workflow, input_data)
    
    assert payload["workflow_type"] == "job_posting_monitor"
    assert payload["extract_fields"] == {"title": "h2", "company": ".company"}


@pytest.mark.asyncio
async def test_workflow_executor_process_page_change_detection():
    """Test processing page change detection result."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    extracted_data = {"html": "<h1>Test</h1>"}
    payload = {
        "workflow_type": "page_change_detection",
        "baseline_content": "old-hash",
        "alert_on_change": False
    }
    
    result = await executor._process_page_change_detection(extracted_data, payload)
    
    assert "changed" in result
    assert "current_hash" in result
    assert "baseline_hash" in result


@pytest.mark.asyncio
async def test_workflow_executor_process_uptime_smoke_check():
    """Test processing uptime smoke check result."""
    from control_plane.job_orchestrator import JobOrchestrator
    
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    extracted_data = {"html": "<h1>Test</h1><div id='main'>Content</div>"}
    payload = {
        "workflow_type": "uptime_smoke_check",
        "required_selectors": ["h1", "#main"],
        "verify_load_time": True,
        "max_load_time_ms": 5000
    }
    job_result = {
        "execution_time": 1.5
    }
    
    result = await executor._process_uptime_smoke_check(extracted_data, payload, job_result)
    
    assert "page_loaded" in result
    assert "load_time_ms" in result
    assert "selectors_found" in result
    assert "status" in result
    assert result["status"] in ["pass", "fail"]


@pytest.mark.asyncio
async def test_workflow_executor_send_webhook():
    """Test sending webhook notification."""
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    # Mock httpx
    with patch("workflows.workflow_executor.httpx_module.AsyncClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client
        
        result = await executor._send_webhook("https://example.com/webhook", {"test": "data"})
        
        assert result is True


@pytest.mark.asyncio
async def test_workflow_executor_send_webhook_failure():
    """Test webhook failure handling."""
    orchestrator = Mock()
    executor = WorkflowExecutor(orchestrator)
    
    # Mock httpx to raise exception
    with patch("workflows.workflow_executor.httpx_module.AsyncClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("Network error"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client
        
        result = await executor._send_webhook("https://example.com/webhook", {"test": "data"})
        
        assert result is False

