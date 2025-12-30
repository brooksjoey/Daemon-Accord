# 04-Control-Plane-Orchestrator/src/control_plane/job_orchestrator.py
import asyncio
import json
import uuid
from typing import Dict, Any, Optional, List, TYPE_CHECKING
from datetime import datetime, timedelta
from enum import Enum
import hashlib

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
import redis.asyncio as redis
import structlog

from ..exceptions import (
    JobExecutionError,
    JobNotFoundError,
    DatabaseError,
    RedisError,
)
from .models import Job, JobExecution
from .queue_manager import QueueManager
from .state_manager import StateManager
from .idempotency_engine import IdempotencyEngine
from .executor_adapter import ExecutorAdapter

if TYPE_CHECKING:
    from ..database import Database
    from core.browser_pool import BrowserPool

logger = structlog.get_logger(__name__)

class JobStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobOrchestrator:
    def __init__(
        self,
        redis_client: redis.Redis,
        db: "Database",
        browser_pool: Optional["BrowserPool"] = None,
        db_session: Optional[AsyncSession] = None,
        max_concurrent_jobs: int = 100,
    ) -> None:
        """
        Initialize Job Orchestrator.
        
        Args:
            redis_client: Redis client for queue and state management
            db: Database instance to get async sessions
            browser_pool: BrowserPool from Execution Engine (optional)
            db_session: AsyncSession for Execution Engine (optional)
            max_concurrent_jobs: Maximum concurrent job executions
        """
        self.redis = redis_client
        self.db = db  # Database instance to get async sessions
        self.db_engine = db.engine  # Keep for backward compatibility if needed
        self.max_concurrent_jobs = max_concurrent_jobs
        
        self.queue_manager = QueueManager(redis_client)
        self.state_manager = StateManager(redis_client, db)
        self.idempotency_engine = IdempotencyEngine(redis_client)
        
        # Execution Engine adapter (lazy initialization)
        self._executor_adapter: Optional[ExecutorAdapter] = None
        self._browser_pool = browser_pool
        self._db_session = db_session
        
        self._running_jobs: Dict[str, asyncio.Task] = {}
        self._workers: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
    
    def _get_executor_adapter(self) -> ExecutorAdapter:
        """
        Get or create executor adapter.
        
        Returns:
            ExecutorAdapter instance
            
        Raises:
            ConfigurationError: If required dependencies are missing
        """
        if self._executor_adapter is None:
            if not self._browser_pool:
                logger.warning(
                    "browser_pool_not_provided",
                    message="Browser pool not provided, Execution Engine may not work"
                )
            if not self._db_session:
                logger.warning(
                    "db_session_not_provided",
                    message="DB session not provided, Execution Engine may not work"
                )
            
            self._executor_adapter = ExecutorAdapter(
                redis_client=self.redis,
                db_session=self._db_session,
                browser_pool=self._browser_pool,
            )
        return self._executor_adapter
        
    async def create_job(
        self,
        domain: str,
        url: str,
        job_type: str,
        strategy: str,
        payload: Dict[str, Any],
        priority: int,
        idempotency_key: Optional[str] = None,
        timeout_seconds: int = 300,
        authorization_mode: str = "public",
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> str:
        """
        Create a new job with idempotency check.
        
        Args:
            authorization_mode: Authorization mode (legacy parameter, kept for compatibility)
            user_id: User/API key ID (legacy parameter)
            ip_address: Request IP address (legacy parameter)
        """
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Check idempotency
        if idempotency_key:
            existing = await self.idempotency_engine.check(idempotency_key)
            if existing:
                logger.info("idempotent_job_found", job_id=existing, idempotency_key=idempotency_key)
                return existing
        
        # Create job record
        job = Job(
            id=job_id,
            domain=domain,
            url=url,
            job_type=job_type,
            strategy=strategy,
            payload=json.dumps(payload),
            priority=priority,
            status=JobStatus.PENDING.value,
            max_attempts=3,
            timeout_seconds=timeout_seconds
        )
        
        # Use async session
        async with self.db.session() as session:
            session.add(job)
            await session.commit()
        
        # Prepare job data for Execution Engine worker
        job_data = {
            "id": job_id,
            "url": url,
            "type": job_type,
            "domain": domain,
            "strategy": strategy,
            "payload": payload,
            "priority": priority,
            "timeout_seconds": timeout_seconds
        }
        
        # Enqueue job with full data for Execution Engine worker
        await self.queue_manager.enqueue(
            job_id=job_id,
            priority=priority,
            domain=domain,
            job_data=job_data
        )
        
        # Store idempotency key if provided
        if idempotency_key:
            await self.idempotency_engine.store(idempotency_key, job_id)
        
        logger.info("job_created", job_id=job_id, domain=domain, job_type=job_type, strategy=strategy)
        return job_id
    
    async def process_job(self, job_id: str):
        """Process a single job."""
        # Get job from DB
        async with self.db.session() as session:
            job = await session.get(Job, job_id)
            if not job:
                logger.error("job_not_found", job_id=job_id)
                raise JobNotFoundError(job_id)
            
            # Update status
            job.status = JobStatus.RUNNING.value
            job.started_at = datetime.utcnow()
            job.attempts += 1
            await session.commit()
        
        # Execute job
        execution_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        try:
            # Call execution engine
            result = await self._execute_job(job)
            
            # Check if execution was successful
            success = result.get("success", False)
            error = result.get("error")
            
            # Process workflow results if this is a workflow job
            workflow_output = None
            if success and job.job_type == "navigate_extract":
                payload = json.loads(job.payload) if job.payload else {}
                workflow_type = payload.get("workflow_type")
                if workflow_type:
                    # Map workflow_type to workflow name
                    workflow_name_map = {
                        "page_change_detection": "page_change_detection",
                        "job_posting_monitor": "job_posting_monitor",
                        "uptime_smoke_check": "uptime_smoke_check"
                    }
                    workflow_name = workflow_name_map.get(workflow_type)
                    if workflow_name:
                        # Import here to avoid circular dependency
                        from ..workflows.workflow_executor import WorkflowExecutor
                        workflow_exec = WorkflowExecutor(self)
                        workflow_output = await workflow_exec.process_workflow_result(
                            workflow_name=workflow_name,
                            job_id=job_id,
                            job_result={
                                "success": success,
                                "data": result.get("data", {}),
                                "artifacts": result.get("artifacts", {}),
                                "payload": payload,
                                "execution_time": result.get("execution_time", 0.0)
                            }
                        )
            
            # Update job status based on execution result
            async with self.db.session() as session:
                job = await session.get(Job, job_id)
                
                if success:
                    job.status = JobStatus.COMPLETED.value
                    job.completed_at = datetime.utcnow()
                    # Use workflow output if available, otherwise use raw result
                    result_data = workflow_output if workflow_output else result.get("data", {})
                    job.result = json.dumps(result_data)
                    job.artifacts = json.dumps(result.get("artifacts", []))
                    job.error = None
                else:
                    # Execution failed - check if we should retry
                    if job.attempts < job.max_attempts:
                        job.status = JobStatus.PENDING.value  # Retry
                        job.error = error
                        logger.warning(
                            "job_failed_will_retry",
                            job_id=job_id,
                            attempts=job.attempts,
                            max_attempts=job.max_attempts,
                            error=error
                        )
                    else:
                        job.status = JobStatus.FAILED.value
                        job.completed_at = datetime.utcnow()
                        job.error = error
                        logger.error(
                            "job_failed_max_attempts",
                            job_id=job_id,
                            attempts=job.attempts,
                            max_attempts=job.max_attempts,
                            error=error
                        )
                
                await session.commit()
            
            # Record execution
            await self._record_execution(
                execution_id=execution_id,
                job_id=job_id,
                attempt=job.attempts,
                status="success" if success else "failed",
                execution_time_ms=int((datetime.utcnow() - start_time).total_seconds() * 1000)
            )
            
            if success:
                logger.info("job_completed_successfully", job_id=job_id)
            else:
                logger.warning("job_execution_failed", job_id=job_id, error=error)
            
        except Exception as e:
            error_msg = str(e)
            logger.error(
                "job_processing_failed",
                job_id=job_id,
                error=error_msg,
                exc_info=True
            )
            raise JobExecutionError(
                f"Job {job_id} processing failed: {error_msg}",
                job_id=job_id
            ) from e
            
            # Update job as failed or retry
            async with self.db.session() as session:
                job = await session.get(Job, job_id)
                job.error = error_msg
                
                if job.attempts >= job.max_attempts:
                    job.status = JobStatus.FAILED.value
                    job.completed_at = datetime.utcnow()
                else:
                    job.status = JobStatus.PENDING.value
                    # Requeue with backoff and job data
                    job_data = {
                        "id": job_id,
                        "url": job.url,
                        "type": job.job_type,
                        "domain": job.domain,
                        "strategy": job.strategy,
                        "payload": json.loads(job.payload) if job.payload else {},
                        "priority": job.priority,
                        "timeout_seconds": job.timeout_seconds
                    }
                    await self.queue_manager.requeue(
                        job_id=job_id,
                        priority=job.priority,
                        domain=job.domain,
                        job_data=job_data,
                        delay_seconds=60 * job.attempts  # Exponential backoff
                    )
                
                await session.commit()
            
            # Record failed execution
            await self._record_execution(
                execution_id=execution_id,
                job_id=job_id,
                attempt=job.attempts,
                status="failed",
                error=error_msg,
                execution_time_ms=int((datetime.utcnow() - start_time).total_seconds() * 1000)
            )
        
        finally:
            # Remove from running jobs
            self._running_jobs.pop(job_id, None)
    
    async def _execute_job(self, job: Job) -> Dict[str, Any]:
        """
        Execute job using Execution Engine.
        
        This method:
        1. Gets the executor adapter
        2. Converts job format
        3. Executes via Execution Engine
        4. Returns result in Control Plane format
        """
        # Parse payload
        payload = json.loads(job.payload) if job.payload else {}
        
        try:
            # Get executor adapter
            adapter = self._get_executor_adapter()
            
            # Execute job via Execution Engine
            result = await adapter.execute_job(
                job_id=job.id,
                domain=job.domain,
                url=job.url,
                job_type=job.job_type,
                strategy=job.strategy,
                payload=payload,
            )
            
            # Return in expected format
            return {
                "success": result.get("success", False),
                "data": result.get("data", {}),
                "artifacts": result.get("artifacts", {}),
                "error": result.get("error"),
                "execution_time": result.get("execution_time", 0.0),
            }
            
        except Exception as e:
            logger.error(
                "job_execution_failed",
                job_id=job.id if hasattr(job, 'id') else 'unknown',
                error=str(e),
                exc_info=True
            )
            raise JobExecutionError(
                f"Job execution failed: {str(e)}",
                job_id=job.id if hasattr(job, 'id') else None
            ) from e
    
    async def _record_execution(self, execution_id: str, job_id: str, attempt: int,
                              status: str, error: Optional[str] = None,
                              execution_time_ms: Optional[int] = None):
        """Record job execution."""
        execution = JobExecution(
            id=execution_id,
            job_id=job_id,
            attempt=attempt,
            status=status,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            execution_time_ms=execution_time_ms,
            error=error
        )
        
        async with self.db.session() as session:
            session.add(execution)
            await session.commit()
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed job status."""
        async with self.db.session() as session:
            job = await session.get(Job, job_id)
            if not job:
                return None
            
            # Get queue position if pending/queued
            queue_position = None
            if job.status in [JobStatus.PENDING.value, JobStatus.QUEUED.value]:
                queue_position = await self.queue_manager.get_position(job_id)
            
            # Parse result if exists
            result = None
            if job.result:
                try:
                    result = json.loads(job.result)
                except:
                    result = {"raw": job.result}
            
            artifacts = []
            if job.artifacts:
                try:
                    artifacts = json.loads(job.artifacts)
                except:
                    artifacts = []
            
            return {
                "job_id": job.id,
                "status": job.status,
                "domain": job.domain,
                "job_type": job.job_type,
                "strategy": job.strategy,
                "priority": job.priority,
                "progress": 100.0 if job.completed_at else 0.0,
                "result": result,
                "error": job.error,
                "started_at": job.started_at,
                "completed_at": job.completed_at,
                "artifacts": artifacts
            }
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job if not running."""
        async with self.db.session() as session:
            job = await session.get(Job, job_id)
            if not job:
                return False
            
            # Can only cancel pending/queued jobs
            if job.status == JobStatus.RUNNING.value:
                return False
            
            # Update status
            job.status = JobStatus.CANCELLED.value
            job.completed_at = datetime.utcnow()
            await session.commit()
        
        # Remove from queue
        await self.queue_manager.remove(job_id)
        
        # Cancel running task if exists
        if job_id in self._running_jobs:
            self._running_jobs[job_id].cancel()
        
        logger.info("job_cancelled", job_id=job_id)
        return True
    
    async def start_worker(self, worker_id: str) -> None:
        """
        Start a worker that processes jobs from queue.
        
        Args:
            worker_id: Unique identifier for this worker
        """
        logger.info("worker_starting", worker_id=worker_id)
        
        # Initialize consumer group for this worker
        await self.queue_manager.initialize_consumer_group(worker_id)
        
        while not self._shutdown_event.is_set():
            try:
                # Get next job from queue
                job_id = await self.queue_manager.dequeue(timeout=5.0)
                if not job_id:
                    await asyncio.sleep(1)
                    continue
                
                # Check if we can run more jobs
                if len(self._running_jobs) >= self.max_concurrent_jobs:
                    await self.queue_manager.requeue(job_id, priority=2, domain="system", delay_seconds=5)
                    await asyncio.sleep(1)
                    continue
                
                # Process job
                task = asyncio.create_task(self.process_job(job_id))
                self._running_jobs[job_id] = task
                
                # Clean up task when done
                def cleanup(_):
                    self._running_jobs.pop(job_id, None)
                
                task.add_done_callback(cleanup)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "worker_error",
                    worker_id=worker_id,
                    error=str(e),
                    exc_info=True
                )
                await asyncio.sleep(5)
        
        logger.info("worker_stopped", worker_id=worker_id)
    
    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        stats = await self.queue_manager.get_stats()
        
        async with self.db.session() as session:
            # Get job counts by status
            statement = select(Job.status, Job.domain)
            result = await session.execute(statement)
            rows = result.all()
            
            status_counts = {}
            domain_counts = {}
            
            for row in rows:
                status = row[0]  # First column: status
                domain = row[1]  # Second column: domain
                status_counts[status] = status_counts.get(status, 0) + 1
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        return {
            "queue": stats,
            "jobs": {
                "total": len(rows),
                "by_status": status_counts,
                "by_domain": domain_counts
            },
            "running_jobs": len(self._running_jobs),
            "workers": len(self._workers)
        }
    
    async def get_queue_depth(self) -> int:
        """Get current queue depth."""
        return await self.queue_manager.get_depth()
    
    async def shutdown(self):
        """Shutdown orchestrator."""
        logger.info("Shutting down orchestrator...")
        import inspect
        
        # Signal workers to stop
        self._shutdown_event.set()
        
        # Cancel all workers
        for worker in self._workers:
            if hasattr(worker, "cancel"):
                worker.cancel()
        
        # Wait for workers to complete
        awaitables = [w for w in self._workers if asyncio.isfuture(w) or inspect.isawaitable(w)]
        if awaitables:
            await asyncio.gather(*awaitables, return_exceptions=True)
        
        # Cancel running jobs
        for job_id, task in list(self._running_jobs.items()):
            if hasattr(task, "cancel"):
                task.cancel()
        
        # Wait for tasks to complete
        running_awaitables = [t for t in self._running_jobs.values() if asyncio.isfuture(t) or inspect.isawaitable(t)]
        if running_awaitables:
            await asyncio.gather(*running_awaitables, return_exceptions=True)
        
        logger.info("Orchestrator shutdown complete")