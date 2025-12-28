"""
Control Plane API

FastAPI application for job orchestration and management.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import structlog
from fastapi import Depends, FastAPI, HTTPException, status, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from .config import ControlPlaneSettings
from .database import Database
from .control_plane.job_orchestrator import JobOrchestrator
from .control_plane.models import JobStatus
from .auth.rate_limiter import RateLimiter, rate_limit_middleware
from .auth.api_key_auth import get_api_key_auth
from .workflows.workflow_registry import get_workflow_registry
from .workflows.workflow_executor import WorkflowExecutor
from .workflows.models import WorkflowInput
from .exceptions import PolicyViolationError, JobExecutionError, JobNotFoundError

# Execution Engine imports (optional - will fail gracefully if not available)
try:
    import sys
    import os
    execution_engine_path = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "01-Core-Execution-Engine", "src"
    )
    if execution_engine_path not in sys.path:
        sys.path.insert(0, execution_engine_path)
    
    from core.browser_pool import BrowserPool
    EXECUTION_ENGINE_AVAILABLE = True
except ImportError:
    # Logger not yet initialized, use print or structlog
    import structlog
    _log = structlog.get_logger(__name__)
    _log.warning("Execution Engine not available - browser automation disabled")
    BrowserPool = None
    EXECUTION_ENGINE_AVAILABLE = False


def setup_logging() -> None:
    """Configure structured logging."""
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )
    logging.basicConfig(level=logging.INFO)


# Initialize settings and logging
settings = ControlPlaneSettings()
setup_logging()
logger = structlog.get_logger(__name__)

# Initialize connections
redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
db = Database(settings)

# Initialize browser pool (if Execution Engine available)
browser_pool: BrowserPool | None = None

# Initialize orchestrator (will be created in lifespan)
orchestrator: JobOrchestrator | None = None

# Initialize rate limiter (will be created in lifespan)
rate_limiter: RateLimiter | None = None

# Initialize workflow executor (will be created in lifespan)
workflow_executor: WorkflowExecutor | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan: startup and shutdown.
    
    - Initialize database tables
    - Create orchestrator
    - Start workers
    - Cleanup on shutdown
    """
    global orchestrator, rate_limiter, workflow_executor
    
    # Startup
    logger.info("control_plane_starting")
    await db.init_models()
    
    # Initialize rate limiter
    rate_limiter = RateLimiter(redis_client=redis_client)
    from .auth.rate_limiter import _rate_limiter as global_rate_limiter
    global_rate_limiter = rate_limiter
    logger.info("rate_limiter_initialized")
    
    # Initialize browser pool (if Execution Engine available)
    global browser_pool
    if EXECUTION_ENGINE_AVAILABLE and BrowserPool:
        browser_pool = BrowserPool(max_instances=20, max_pages_per_instance=5)
        await browser_pool.initialize()
        logger.info("browser_pool_initialized")
    else:
        logger.warning("browser_pool_not_available")
    
    # Initialize policy enforcer
    from .compliance.policy_enforcer import PolicyEnforcer
    policy_enforcer = PolicyEnforcer(
        db_session_factory=db.session,
        redis_client=redis_client,
    )
    logger.info("policy_enforcer_initialized")
    
    # Create orchestrator
    orchestrator = JobOrchestrator(
        redis_client=redis_client,
        db=db,  # Pass Database instance (not just engine)
        browser_pool=browser_pool,
        db_session=db.session(),  # AsyncSession for Execution Engine
        max_concurrent_jobs=settings.max_concurrent_jobs,
        policy_enforcer=policy_enforcer,
    )
    
    # Initialize workflow executor
    workflow_executor = WorkflowExecutor(orchestrator)
    logger.info("workflow_executor_initialized")
    
    # NOTE: Workers are disabled in containerized deployments
    # The Execution Engine worker service handles job execution via Redis Streams
    # Control Plane only enqueues jobs, does not process them
    # 
    # If you need Control Plane to process jobs (local dev without Execution Engine container):
    # Uncomment the following lines:
    #
    # for i in range(settings.worker_count):
    #     worker_id = f"worker-{i+1}"
    #     task = asyncio.create_task(orchestrator.start_worker(worker_id))
    #     orchestrator._workers.append(task)
    #     logger.info("worker_started", worker_id=worker_id)
    
    logger.info("control_plane_ready", workers=0, note="Execution Engine worker handles job processing")
    
    yield
    
    # Shutdown
    logger.info("control_plane_shutting_down")
    if orchestrator:
        await orchestrator.shutdown()
    
    # Cleanup browser pool
    if browser_pool and hasattr(browser_pool, 'playwright') and browser_pool.playwright:
        await browser_pool.playwright.stop()
        logger.info("browser_pool_stopped")
    
    await db.dispose()
    await redis_client.aclose()
    logger.info("control_plane_stopped")


# Create FastAPI app
app = FastAPI(
    title="Control Plane Orchestrator API",
    description="""
    Job orchestration and management API for Daemon Accord.
    
    ## Features
    
    * **Job Management**: Create, monitor, and cancel jobs
    * **Queue Management**: Priority-based job queuing with Redis Streams
    * **Idempotency**: Prevent duplicate job creation
    * **Status Tracking**: Real-time job status and progress monitoring
    
    ## Authentication
    
    Currently authentication is disabled for development. In production, use API keys or JWT tokens.
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


def get_orchestrator() -> JobOrchestrator:
    """Dependency to get orchestrator instance."""
    if orchestrator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Orchestrator not initialized"
        )
    return orchestrator


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "control-plane",
        "workers": settings.worker_count,
    }


# Job creation endpoint
@app.post("/api/v1/jobs", status_code=status.HTTP_201_CREATED)
async def create_job(
    request: Request,
    domain: str,
    url: str,
    job_type: str,
    strategy: str = "vanilla",
    priority: int = 2,
    payload: dict = {},
    idempotency_key: str | None = None,
    timeout_seconds: int = 300,
    authorization_mode: str = "public",
    orch: JobOrchestrator = Depends(get_orchestrator),
):
    """
    Create a new job with policy enforcement.
    
    Args:
        domain: Target domain (e.g., 'amazon.com')
        url: Target URL
        job_type: Type of job ('navigate_extract', 'authenticate', etc.)
        strategy: Execution strategy ('vanilla', 'stealth', 'assault')
        priority: Priority level (0=emergency, 1=high, 2=normal, 3=low)
        payload: Job-specific payload data
        idempotency_key: Optional idempotency key to prevent duplicates
        timeout_seconds: Job timeout in seconds
        authorization_mode: 'public', 'customer-authorized', or 'internal'
        
    Returns:
        Job ID and status
    """
    # Get user ID and IP address for audit logging
    user_id = None
    ip_address = request.client.host if request.client else None
    
    # Extract API key from header if available
    api_key = request.headers.get("X-API-Key")
    if api_key:
        user_id = f"api_key:{api_key[:8]}..."  # Truncated for privacy
    
    try:
        job_id = await orch.create_job(
            domain=domain,
            url=url,
            job_type=job_type,
            strategy=strategy,
            payload=payload,
            priority=priority,
            idempotency_key=idempotency_key,
            timeout_seconds=timeout_seconds,
            authorization_mode=authorization_mode,
            user_id=user_id,
            ip_address=ip_address,
        )
        
        return {
            "job_id": job_id,
            "status": "created",
            "domain": domain,
            "job_type": job_type,
        }
        
    except PolicyViolationError as e:
        # Policy violation - return 403
        logger.warning(
            "job_creation_policy_violation",
            error=str(e),
            domain=domain,
            policy_action=e.policy_action,
            details=e.details
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Policy violation: {str(e)}"
        )
    except ValueError as e:
        # Other validation errors
        logger.warning("job_creation_validation_error", error=str(e), domain=domain)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error("job_creation_failed", error=str(e), domain=domain)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create job: {str(e)}"
        )


# Job status endpoint
@app.get("/api/v1/jobs/{job_id}")
async def get_job_status(
    job_id: str,
    orch: JobOrchestrator = Depends(get_orchestrator),
):
    """
    Get job status and details.
    
    Args:
        job_id: The job ID
        
    Returns:
        Job status and details
    """
    try:
        status_info = await orch.get_job_status(job_id)
    except JobNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    
    return status_info


# Queue stats endpoint
@app.get("/api/v1/queue/stats")
async def get_queue_stats(
    orch: JobOrchestrator = Depends(get_orchestrator),
):
    """Get queue statistics."""
    stats = await orch.get_queue_stats()
    return stats


# Operator Dashboard endpoint
@app.get("/api/v1/ops/status")
async def get_ops_status(
    orch: JobOrchestrator = Depends(get_orchestrator),
):
    """
    Operator Dashboard - System health and operational metrics.
    
    Returns:
        - Health status
        - Queue depth
        - Recent jobs (last 10)
        - Success rate (last 100 jobs)
    """
    from datetime import datetime, timedelta
    from sqlmodel import select, func
    from .control_plane.models import Job, JobStatus
    
    try:
        # Get health status
        health_status = "healthy"
        try:
            await redis_client.ping()
            db_status = "connected"
        except Exception:
            health_status = "degraded"
            db_status = "disconnected"
        
        # Get queue stats
        queue_stats = await orch.get_queue_stats()
        queue_depth = queue_stats.get("total", 0)
        
        # Get recent jobs (last 10)
        recent_jobs = []
        async with db.session() as session:
            statement = select(Job).order_by(Job.created_at.desc()).limit(10)
            result = await session.execute(statement)
            jobs = result.scalars().all()
            
            for job in jobs:
                recent_jobs.append({
                    "job_id": job.id,
                    "status": job.status,
                    "domain": job.domain,
                    "job_type": job.job_type,
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                })
        
        # Calculate success rate (last 100 jobs)
        success_rate = None
        total_jobs = 0
        successful_jobs = 0
        
        async with db.session() as session:
            # Get last 100 completed or failed jobs
            statement = select(Job).where(
                (Job.status == JobStatus.COMPLETED) | (Job.status == JobStatus.FAILED)
            ).order_by(Job.completed_at.desc()).limit(100)
            result = await session.execute(statement)
            jobs = result.scalars().all()
            
            total_jobs = len(jobs)
            successful_jobs = sum(1 for job in jobs if job.status == JobStatus.COMPLETED)
            
            if total_jobs > 0:
                success_rate = round((successful_jobs / total_jobs) * 100, 2)
        
        return {
            "health": {
                "status": health_status,
                "database": db_status,
                "redis": "connected" if health_status == "healthy" else "disconnected",
                "timestamp": datetime.utcnow().isoformat(),
            },
            "queue": {
                "depth": queue_depth,
                "by_priority": {
                    "emergency": queue_stats.get("emergency", {}).get("length", 0),
                    "high": queue_stats.get("high", {}).get("length", 0),
                    "normal": queue_stats.get("normal", {}).get("length", 0),
                    "low": queue_stats.get("low", {}).get("length", 0),
                },
                "delayed": queue_stats.get("delayed", {}).get("count", 0),
                "dlq": queue_stats.get("dlq", {}).get("length", 0),
            },
            "recent_jobs": recent_jobs,
            "metrics": {
                "success_rate_percent": success_rate,
                "total_jobs_sampled": total_jobs,
                "successful_jobs": successful_jobs,
                "failed_jobs": total_jobs - successful_jobs,
            },
            "system": {
                "worker_count": settings.worker_count,
                "max_concurrent_jobs": settings.max_concurrent_jobs,
            },
        }
    except Exception as e:
        logger.error("ops_status_error", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get operational status: {str(e)}"
        )


# Workflow endpoints
def get_workflow_executor() -> WorkflowExecutor:
    """Dependency to get workflow executor instance."""
    if workflow_executor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Workflow executor not initialized"
        )
    return workflow_executor


@app.get("/api/v1/workflows")
async def list_workflows(
    executor: WorkflowExecutor = Depends(get_workflow_executor),
):
    """
    List all available workflow templates.
    
    Returns:
        Dictionary of workflow names to workflow definitions
    """
    registry = get_workflow_registry()
    return registry.get_summary()


@app.get("/api/v1/workflows/{workflow_name}")
async def get_workflow(
    workflow_name: str,
    executor: WorkflowExecutor = Depends(get_workflow_executor),
):
    """
    Get details of a specific workflow template.
    
    Args:
        workflow_name: Name of the workflow
        
    Returns:
        Workflow definition with input/output schemas
    """
    registry = get_workflow_registry()
    workflow = registry.get(workflow_name)
    
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow '{workflow_name}' not found"
        )
    
    return {
        "name": workflow.name,
        "display_name": workflow.display_name,
        "description": workflow.description,
        "input_schema": workflow.input_schema,
        "output_schema": workflow.output_schema,
        "execution_steps": workflow.execution_steps,
        "default_strategy": workflow.default_strategy,
    }


@app.post("/api/v1/workflows/{workflow_name}/run", status_code=status.HTTP_201_CREATED)
async def run_workflow(
    workflow_name: str,
    input_data: WorkflowInput,
    executor: WorkflowExecutor = Depends(get_workflow_executor),
):
    """
    Execute a workflow template.
    
    Args:
        workflow_name: Name of workflow to execute
        input_data: Workflow input data (validated against workflow schema)
        
    Returns:
        Workflow result with job ID and status
    """
    try:
        # Convert Pydantic model to dict
        input_dict = input_data.model_dump(exclude_none=True)
        
        # Add webhook URL if provided
        if input_data.webhook_url:
            input_dict["webhook_url"] = input_data.webhook_url
        
        # Execute workflow
        result = await executor.execute_workflow(
            workflow_name=workflow_name,
            input_data=input_dict,
            webhook_url=input_data.webhook_url
        )
        
        return {
            "workflow_name": result.workflow_name,
            "job_id": result.job_id,
            "status": result.status.value,
            "created_at": result.created_at.isoformat(),
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except PolicyViolationError as e:
        logger.warning(
            "workflow_policy_violation",
            workflow_name=workflow_name,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Policy violation: {str(e)}"
        )
    except JobExecutionError as e:
        logger.error(
            "workflow_execution_failed",
            workflow_name=workflow_name,
            job_id=e.job_id,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute workflow: {str(e)}"
        )
    except Exception as e:
        logger.error(
            "workflow_execution_failed",
            workflow_name=workflow_name,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute workflow: {str(e)}"
        )


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "control-plane",
        "version": "1.0.0",
        "status": "operational",
    }


# For running directly with python -m
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
